use crate::StoreError;
use dsh_session_format::{parse_header, Event, FormatError, Header};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use zstd_safe::{CCtx, CParameter, DCtx, InBuffer, OutBuffer};

const ZSTD_MAGIC: u32 = 0xFD2F_B528;
const IO_BUFFER: usize = 64 * 1024;

pub(crate) trait EventSink {
    fn accept(&mut self, event: Event) -> Result<(), StoreError>;
}

pub(crate) struct CollectEvents<'a>(pub &'a mut Vec<Event>);

impl EventSink for CollectEvents<'_> {
    fn accept(&mut self, event: Event) -> Result<(), StoreError> {
        self.0.push(event);
        Ok(())
    }
}

pub(crate) struct CallbackEvents<F>(pub F);

impl<F: FnMut(Event) -> Result<(), StoreError>> EventSink for CallbackEvents<F> {
    fn accept(&mut self, event: Event) -> Result<(), StoreError> {
        (self.0)(event)
    }
}

#[derive(Debug)]
pub(crate) struct DecodedLog {
    pub header: Header,
    pub event_count: usize,
    /// Physical byte offset of the durable prefix.
    pub committed_bytes: u64,
    /// A structurally incomplete final zstd frame needs atomic republication.
    pub repair_required: bool,
}

#[derive(Default)]
struct EventStats {
    committed_bytes: u64,
    input_bytes: u64,
    event_count: usize,
}

fn header_record<R: BufRead>(reader: &mut R) -> Result<(Header, Vec<u8>), StoreError> {
    let mut line = Vec::new();
    if reader.read_until(b'\n', &mut line)? == 0 || line.last() != Some(&b'\n') {
        return Err(FormatError::MissingHeader.into());
    }
    let header = parse_header(&line[..line.len() - 1])?;
    Ok((header, line))
}

fn scan_events<R: Read>(
    reader: R,
    header_bytes: u64,
    sink: &mut impl EventSink,
) -> Result<EventStats, StoreError> {
    let mut reader = BufReader::with_capacity(IO_BUFFER, reader);
    let mut stats = EventStats {
        committed_bytes: header_bytes,
        input_bytes: header_bytes,
        event_count: 0,
    };
    let mut line = Vec::new();
    let mut line_no = 0usize;
    let mut issue: Option<FormatError> = None;
    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        stats.input_bytes += read as u64;
        if line.last() != Some(&b'\n') {
            break;
        }
        line_no += 1;
        let value = serde_json::from_slice::<serde_json::Value>(&line);
        let is_turn_end = value
            .as_ref()
            .ok()
            .and_then(|value| value.get("type"))
            .and_then(serde_json::Value::as_str)
            == Some("turn/end");
        let parsed: Result<Event, FormatError> = match value {
            Err(_) => Err(FormatError::Unparsable(line_no)),
            Ok(value) => serde_json::from_value(value).map_err(|error| FormatError::InvalidEvent {
                line: line_no,
                reason: error.to_string(),
            }),
        };
        if let Some(existing) = issue.take() {
            if is_turn_end {
                let error = match parsed {
                    Ok(_) => existing,
                    Err(error) => error,
                };
                return Err(error.into());
            }
            issue = Some(existing);
            continue;
        }
        match parsed {
            Ok(event) => {
                sink.accept(event)?;
                stats.event_count += 1;
                stats.committed_bytes = stats.input_bytes;
            }
            Err(error) if is_turn_end => return Err(error.into()),
            Err(error) => issue = Some(error),
        }
    }
    Ok(stats)
}

pub(crate) fn read_plaintext(
    path: &std::path::Path,
    sink: &mut impl EventSink,
) -> Result<DecodedLog, StoreError> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(IO_BUFFER, file);
    let (header, record) = header_record(&mut reader)?;
    let stats = scan_events(reader, record.len() as u64, sink)?;
    Ok(DecodedLog {
        header,
        event_count: stats.event_count,
        committed_bytes: stats.committed_bytes,
        repair_required: stats.committed_bytes != stats.input_bytes,
    })
}

#[derive(Debug, Clone, Copy)]
struct FrameScan {
    first_end: u64,
    complete_end: u64,
    torn_start: Option<u64>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Piece {
    Complete,
    Eof,
    Torn,
}

fn read_piece<R: Read>(reader: &mut R, bytes: &mut [u8]) -> io::Result<Piece> {
    let mut filled = 0;
    while filled < bytes.len() {
        match reader.read(&mut bytes[filled..]) {
            Ok(0) => return Ok(if filled == 0 { Piece::Eof } else { Piece::Torn }),
            Ok(count) => filled += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(Piece::Complete)
}

fn skip_payload<R: Read>(reader: &mut R, mut remaining: u64) -> io::Result<Piece> {
    let mut buffer = [0u8; IO_BUFFER];
    while remaining != 0 {
        let wanted = remaining.min(buffer.len() as u64) as usize;
        if read_piece(reader, &mut buffer[..wanted])? != Piece::Complete {
            return Ok(Piece::Torn);
        }
        remaining -= wanted as u64;
    }
    Ok(Piece::Complete)
}

/// Locate frame boundaries without decompressing or buffering the artifact.
fn scan_frames(file: &mut File, max_frames: usize) -> Result<FrameScan, StoreError> {
    let mut reader = BufReader::with_capacity(IO_BUFFER, file);
    let mut first_end = 0;
    let mut complete_end = 0;
    let mut frames = 0usize;
    loop {
        let start = reader.stream_position()?;
        let mut magic = [0u8; 4];
        match read_piece(&mut reader, &mut magic)? {
            Piece::Complete => {}
            Piece::Eof => {
                return Ok(FrameScan {
                    first_end,
                    complete_end,
                    torn_start: None,
                })
            }
            Piece::Torn => {
                return Ok(FrameScan {
                    first_end,
                    complete_end,
                    torn_start: Some(start),
                })
            }
        }
        if u32::from_le_bytes(magic) != ZSTD_MAGIC {
            return Err(StoreError::Corrupt(format!(
                "invalid zstd frame magic at byte {start}"
            )));
        }
        let mut descriptor = [0u8; 1];
        if read_piece(&mut reader, &mut descriptor)? != Piece::Complete {
            return Ok(FrameScan {
                first_end,
                complete_end,
                torn_start: Some(start),
            });
        }
        let descriptor = descriptor[0];
        if descriptor & 0x18 != 0 {
            return Err(StoreError::Corrupt(format!(
                "reserved zstd frame-header bit at byte {start}"
            )));
        }
        let content_size_flag = descriptor >> 6;
        let single_segment = descriptor & 0x20 != 0;
        let checksum = descriptor & 0x04 != 0;
        let dictionary_bytes = match descriptor & 0x03 {
            3 => 4,
            value => value as usize,
        };
        let content_size_bytes = if content_size_flag == 0 {
            usize::from(single_segment)
        } else {
            1 << content_size_flag
        };
        let header_bytes = usize::from(!single_segment) + dictionary_bytes + content_size_bytes;
        let mut extra = vec![0u8; header_bytes];
        if read_piece(&mut reader, &mut extra)? != Piece::Complete {
            return Ok(FrameScan {
                first_end,
                complete_end,
                torn_start: Some(start),
            });
        }
        loop {
            let mut block = [0u8; 3];
            if read_piece(&mut reader, &mut block)? != Piece::Complete {
                return Ok(FrameScan {
                    first_end,
                    complete_end,
                    torn_start: Some(start),
                });
            }
            let header =
                u32::from(block[0]) | (u32::from(block[1]) << 8) | (u32::from(block[2]) << 16);
            let last = header & 1 != 0;
            let block_type = (header >> 1) & 0x03;
            let block_size = header >> 3;
            if block_type == 3 {
                return Err(StoreError::Corrupt(format!(
                    "reserved zstd block type at byte {start}"
                )));
            }
            let payload = if block_type == 1 {
                1
            } else {
                block_size as u64
            };
            if skip_payload(&mut reader, payload)? != Piece::Complete {
                return Ok(FrameScan {
                    first_end,
                    complete_end,
                    torn_start: Some(start),
                });
            }
            if last {
                break;
            }
        }
        if checksum && read_piece(&mut reader, &mut [0u8; 4])? != Piece::Complete {
            return Ok(FrameScan {
                first_end,
                complete_end,
                torn_start: Some(start),
            });
        }
        complete_end = reader.stream_position()?;
        frames += 1;
        if frames == 1 {
            first_end = complete_end;
        }
        if frames == max_frames {
            return Ok(FrameScan {
                first_end,
                complete_end,
                torn_start: None,
            });
        }
    }
}

pub(crate) fn read_zstd_header(path: &std::path::Path) -> Result<(Header, u64), StoreError> {
    let mut file = File::open(path)?;
    let scan = scan_frames(&mut file, 1)?;
    if scan.first_end == 0 {
        return Err(FormatError::MissingHeader.into());
    }
    file.seek(SeekFrom::Start(0))?;
    let decoder = zstd::stream::read::Decoder::new((&file).take(scan.first_end))?;
    let mut decoder = io::BufReader::new(decoder);
    let (header, record) = header_record(&mut decoder)?;
    let mut extra = [0u8; 1];
    if decoder.read(&mut extra)? != 0 {
        return Err(StoreError::Corrupt(
            "zstd header frame contains more than one JSONL record".into(),
        ));
    }
    Ok((header, record.len() as u64))
}

/// Streaming decoder for the available prefix of one incomplete frame.
struct PrefixReader {
    decoder: DCtx<'static>,
    reader: BufReader<File>,
    input: Vec<u8>,
    input_pos: usize,
    input_len: usize,
    output: Vec<u8>,
    output_pos: usize,
    output_len: usize,
    eof: bool,
    finished: bool,
}

impl PrefixReader {
    fn new(file: File) -> io::Result<Self> {
        let mut decoder = DCtx::create();
        decoder
            .init()
            .map_err(|code| io::Error::other(format!("zstd init failed: {code}")))?;
        Ok(Self {
            decoder,
            reader: BufReader::with_capacity(IO_BUFFER, file),
            input: vec![0u8; IO_BUFFER],
            input_pos: 0,
            input_len: 0,
            output: vec![0u8; DCtx::out_size()],
            output_pos: 0,
            output_len: 0,
            eof: false,
            finished: false,
        })
    }

    fn decode_step(&mut self, flush: bool) -> io::Result<()> {
        self.output_pos = 0;
        self.output_len = 0;
        let mut output = OutBuffer::around(&mut self.output);
        let mut input = InBuffer::around(if flush {
            &[][..]
        } else {
            &self.input[self.input_pos..self.input_len]
        });
        let remaining = self
            .decoder
            .decompress_stream(&mut output, &mut input)
            .map_err(|code| io::Error::other(format!("zstd decode failed: {code}")))?;
        self.input_pos += input.pos();
        self.output_len = output.pos();
        // Empty-input decoding drains every complete compressed block. No
        // progress in an incomplete final frame means recovery is exhausted.
        if flush && self.output_len == 0 {
            self.finished = true;
        }
        let _ = remaining;
        Ok(())
    }
}

impl Read for PrefixReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            if self.output_pos < self.output_len {
                let count = (self.output_len - self.output_pos).min(buffer.len());
                buffer[..count]
                    .copy_from_slice(&self.output[self.output_pos..self.output_pos + count]);
                self.output_pos += count;
                return Ok(count);
            }
            if self.finished {
                return Ok(0);
            }
            if self.input_pos == self.input_len {
                if self.eof {
                    self.decode_step(true)?;
                    continue;
                }
                self.input_len = self.reader.read(&mut self.input)?;
                self.input_pos = 0;
                if self.input_len == 0 {
                    self.eof = true;
                    continue;
                }
            }
            self.decode_step(false)?;
        }
    }
}

pub(crate) fn read_zstd(
    path: &std::path::Path,
    sink: &mut impl EventSink,
) -> Result<DecodedLog, StoreError> {
    let mut file = File::open(path)?;
    let scan = scan_frames(&mut file, usize::MAX)?;
    if scan.first_end == 0 {
        return Err(FormatError::MissingHeader.into());
    }
    file.seek(SeekFrom::Start(0))?;
    let header_decoder = zstd::stream::read::Decoder::new((&file).take(scan.first_end))?;
    let mut header_decoder = io::BufReader::new(header_decoder);
    let (header, record) = header_record(&mut header_decoder)?;
    let mut extra = [0u8; 1];
    if header_decoder.read(&mut extra)? != 0 {
        return Err(StoreError::Corrupt(
            "zstd header frame contains more than one JSONL record".into(),
        ));
    }
    drop(header_decoder);

    file.seek(SeekFrom::Start(scan.first_end))?;
    let body_len = scan.complete_end - scan.first_end;
    let stats = if body_len == 0 {
        scan_events(io::empty(), record.len() as u64, sink)?
    } else {
        let body = zstd::stream::read::Decoder::new((&file).take(body_len))?;
        scan_events(body, record.len() as u64, sink)?
    };
    if stats.committed_bytes != stats.input_bytes {
        return Err(StoreError::Corrupt(
            "complete zstd frame contains a torn or corrupt JSONL record".into(),
        ));
    }
    let mut event_count = stats.event_count;
    if let Some(torn_start) = scan.torn_start {
        file.seek(SeekFrom::Start(torn_start))?;
        let prefix = PrefixReader::new(file);
        // Match the TypeScript recovery rule: a prefix the decoder cannot emit is
        // discarded, while every complete prior frame remains durable.
        if let Ok(prefix) = prefix {
            let mut recovered_events = Vec::new();
            let mut recovered = CollectEvents(&mut recovered_events);
            if scan_events(prefix, 0, &mut recovered).is_ok() {
                for event in recovered_events {
                    sink.accept(event)?;
                    event_count += 1;
                }
            }
        }
    }
    Ok(DecodedLog {
        header,
        event_count,
        committed_bytes: scan.complete_end,
        repair_required: scan.torn_start.is_some(),
    })
}

pub(crate) fn compress_frame(input: &[u8]) -> io::Result<Vec<u8>> {
    let mut context = CCtx::create();
    context
        .set_parameter(CParameter::ChecksumFlag(true))
        .map_err(|code| io::Error::other(format!("zstd checksum configuration failed: {code}")))?;
    let mut output = vec![0u8; zstd_safe::compress_bound(input.len())];
    let written = context
        .compress2(&mut output, input)
        .map_err(|code| io::Error::other(format!("zstd compression failed: {code}")))?;
    output.truncate(written);
    Ok(output)
}
