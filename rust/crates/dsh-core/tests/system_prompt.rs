use dsh_core::system_prompt::{
    assemble_agent_prompt, tool_guidance, PromptSection, SystemPromptAssembler, SystemPromptError,
    ToolPromptSection, IDENTITY,
};
use dsh_core::{BoxFuture, Tool, ToolContext, ToolOutput, ToolRegistry};
use dsh_llm::ToolDef;
use serde_json::{json, Value};

fn tool(name: &str, description: &str) -> ToolDef {
    ToolDef {
        name: name.into(),
        description: description.into(),
        input_schema: json!({ "type": "object", "properties": {} }),
    }
}

fn names(tools: &[ToolDef]) -> Vec<&str> {
    tools.iter().map(|tool| tool.name.as_str()).collect()
}

#[test]
fn empty_inputs_produce_an_empty_assembly() {
    let assembly = SystemPromptAssembler::default()
        .assemble(Vec::new())
        .unwrap();

    assert_eq!(assembly.prompt, "");
    assert!(assembly.tools.is_empty());

    let assembly = SystemPromptAssembler::new(" \n\t ")
        .with_section(PromptSection::new("blank", 0.0, " \n "))
        .assemble(Vec::new())
        .unwrap();
    assert_eq!(assembly.prompt, "");
}

#[test]
fn sections_are_ordered_trimmed_and_joined_without_rewriting_interior_text() {
    let assembler = SystemPromptAssembler::new("  Base instructions.\nKeep  this.  ")
        .with_section(PromptSection::new(
            "later",
            20.0,
            "  Later section.\n\nSecond paragraph.  ",
        ))
        .with_section(PromptSection::new("zeta", 10.0, "zeta"))
        .with_section(PromptSection::new("alpha", 10.0, " alpha "))
        .with_section(PromptSection::new("fractional", 10.5, "fractional"))
        .with_section(PromptSection::new("first", -10.0, " first "))
        .with_section(PromptSection::new("blank", 0.0, "\n\t"));

    let assembly = assembler.assemble(Vec::new()).unwrap();

    assert_eq!(
        assembly.prompt,
        "Base instructions.\nKeep  this.\n\nfirst\n\nalpha\n\nzeta\n\nfractional\n\nLater section.\n\nSecond paragraph."
    );
}

#[test]
fn equal_section_orders_use_utf16_code_unit_name_order() {
    for (left, right) in [("zeta", "äther"), ("äther", "zeta")] {
        let assembly = SystemPromptAssembler::new("base")
            .with_section(PromptSection::new(left, 10.0, left))
            .with_section(PromptSection::new(right, 10.0, right))
            .assemble(Vec::new())
            .unwrap();
        assert_eq!(assembly.prompt, "base\n\nzeta\n\näther");
    }

    // U+E000 sorts before U+1F600 by Unicode scalar, but after it by UTF-16
    // code units. TypeScript's string comparison uses the latter.
    let assembly = SystemPromptAssembler::new("base")
        .with_section(PromptSection::new("\u{e000}", 10.0, "private-use"))
        .with_section(PromptSection::new("😀", 10.0, "emoji"))
        .assemble(Vec::new())
        .unwrap();
    assert_eq!(assembly.prompt, "base\n\nemoji\n\nprivate-use");
}

#[test]
fn tool_guidance_is_omitted_for_absent_and_explicitly_disabled_tools() {
    let assembler = SystemPromptAssembler::new("base")
        .with_tool_section(ToolPromptSection::for_tool(
            "write",
            20.0,
            "Write guidance.",
        ))
        .with_tool_section(ToolPromptSection::for_tool("read", 10.0, "Read guidance."))
        .with_tool_section(ToolPromptSection::for_tool(
            "missing",
            0.0,
            "Missing guidance.",
        ))
        .with_section(PromptSection::new("policy", 30.0, "Policy guidance."))
        .disable_tool("write")
        .disable_tools(["not-registered"]);

    let assembly = assembler
        .assemble([tool("write", "write"), tool("read", "read")])
        .unwrap();

    assert_eq!(
        assembly.prompt,
        "base\n\nRead guidance.\n\nPolicy guidance."
    );
    assert_eq!(names(&assembly.tools), ["read"]);
}

#[test]
fn disabling_a_tool_is_exact_and_does_not_remove_similarly_named_tools() {
    let assembly = SystemPromptAssembler::new("base")
        .with_tool_section(ToolPromptSection::for_tool("read", 0.0, "read guidance"))
        .with_tool_section(ToolPromptSection::for_tool(
            "read_image",
            0.0,
            "image guidance",
        ))
        .disable_tool("read")
        .assemble([tool("read", "read"), tool("read_image", "image")])
        .unwrap();

    assert_eq!(assembly.prompt, "base\n\nimage guidance");
    assert_eq!(names(&assembly.tools), ["read_image"]);
}

#[test]
fn tool_sorting_is_stable_for_duplicate_names() {
    let tools = vec![
        tool("dup", "first"),
        tool("anchor", "anchor"),
        tool("dup", "second"),
        tool("zulu", "zulu"),
    ];

    let assembly = SystemPromptAssembler::default()
        .assemble(tools.clone())
        .unwrap();
    assert_eq!(names(&assembly.tools), ["anchor", "dup", "dup", "zulu"]);
    assert_eq!(assembly.tools[1].description, "first");
    assert_eq!(assembly.tools[2].description, "second");

    let mut reversed = tools;
    reversed.reverse();
    let assembly = SystemPromptAssembler::default().assemble(reversed).unwrap();
    assert_eq!(names(&assembly.tools), ["anchor", "dup", "dup", "zulu"]);
    assert_eq!(assembly.tools[1].description, "second");
    assert_eq!(assembly.tools[2].description, "first");
}

#[test]
fn unusual_tool_names_are_preserved_and_sorted_by_utf16_code_units() {
    let unusual = [
        "",
        " with spaces ",
        "read/write",
        "read.write",
        "工具",
        "äther",
        "\u{e000}",
        "😀",
    ];
    let tools: Vec<ToolDef> = unusual.iter().rev().map(|name| tool(name, name)).collect();

    let assembly = SystemPromptAssembler::default().assemble(tools).unwrap();

    assert_eq!(
        names(&assembly.tools),
        [
            "",
            " with spaces ",
            "read.write",
            "read/write",
            "äther",
            "工具",
            "😀",
            "\u{e000}"
        ]
    );
    for name in unusual {
        assert!(assembly.tools.iter().any(|tool| tool.name == name));
    }
}

#[test]
fn large_inputs_and_long_names_are_not_truncated() {
    let mut tools: Vec<ToolDef> = (0..2048)
        .rev()
        .map(|index| tool(&format!("tool-{index:04}"), &format!("description-{index}")))
        .collect();
    let long_name = "名前-".repeat(4096);
    tools.push(tool(&long_name, "long-name"));
    tools.push(tool("anchor", "anchor"));

    let long_text = "instruction ".repeat(8192);
    let assembly = SystemPromptAssembler::new("base")
        .with_section(PromptSection::new("large", 0.0, &long_text))
        .assemble(tools)
        .unwrap();

    assert_eq!(assembly.prompt, format!("base\n\n{}", long_text.trim()));
    assert_eq!(assembly.tools.len(), 2050);
    assert_eq!(assembly.tools[0].name, "anchor");
    assert_eq!(assembly.tools[1].name, "tool-0000");
    assert_eq!(assembly.tools[2048].name, "tool-2047");
    assert_eq!(assembly.tools[2049].name, long_name);
    assert_eq!(assembly.tools[2049].description, "long-name");
}

#[test]
fn every_non_finite_section_order_is_rejected_before_filtering() {
    for order in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let error = SystemPromptAssembler::new("base")
            .with_tool_section(ToolPromptSection::for_tool("disabled", order, "hidden"))
            .disable_tool("disabled")
            .assemble(Vec::new())
            .unwrap_err();
        assert_eq!(
            error,
            SystemPromptError::NonFiniteSectionOrder {
                name: "tool:disabled".into()
            }
        );
        assert_eq!(
            error.to_string(),
            "prompt section \"tool:disabled\" order must be a finite number"
        );

        let error = SystemPromptAssembler::new("base")
            .with_section(PromptSection::new("invalid", order, "hidden"))
            .assemble(Vec::new())
            .unwrap_err();
        assert_eq!(
            error,
            SystemPromptError::NonFiniteSectionOrder {
                name: "invalid".into()
            }
        );
    }
}

struct StubTool(ToolDef);

impl Tool for StubTool {
    fn def(&self) -> ToolDef {
        self.0.clone()
    }

    fn call<'a>(&'a self, _args: Value, _cx: &'a ToolContext) -> BoxFuture<'a, ToolOutput> {
        Box::pin(async { ToolOutput::ok("unused") })
    }
}

#[test]
fn registry_definitions_can_be_assembled_without_changing_the_registry() {
    let mut registry = ToolRegistry::default();
    registry.register(StubTool(tool("zulu", "zulu")));
    registry.register(StubTool(tool("alpha", "alpha")));
    let before = registry.defs();

    let assembly = SystemPromptAssembler::new("base")
        .with_tool_section(ToolPromptSection::for_tool("zulu", 0.0, "zulu guidance"))
        .disable_tool("alpha")
        .assemble_registry(&registry)
        .unwrap();

    assert_eq!(assembly.prompt, "base\n\nzulu guidance");
    assert_eq!(names(&assembly.tools), ["zulu"]);
    assert_eq!(registry.defs(), before);
}
#[test]
fn agent_prompt_starts_with_identity_then_persona_then_instructions() {
    let prompt = assemble_agent_prompt(
        "You are a coding agent powered by the test model.",
        "The following workspace instructions may be relevant.\n\nInstructions from: AGENTS.md\n\nBe precise.",
        &["read".to_string(), "bash".to_string()],
    );
    let expected_start = concat!(
        "You are an AI agent powered by DeepSeek Harness.\n\n",
        "You are a coding agent powered by the test model.\n\n",
        "The following workspace instructions may be relevant.\n\nInstructions from: AGENTS.md\n\nBe precise.\n\n"
    );
    assert!(
        prompt.starts_with(expected_start),
        "unexpected prompt: {prompt}"
    );
}

#[test]
fn agent_prompt_omits_blank_persona_and_instructions() {
    let prompt = assemble_agent_prompt("  ", "\n\t", &[]);
    assert_eq!(prompt, IDENTITY);
}

#[test]
fn tool_guidance_follows_tool_order_and_skips_unknown_tools() {
    let prompt = assemble_agent_prompt(
        "persona",
        "",
        &[
            "unknown".to_string(),
            "bash".to_string(),
            "read".to_string(),
        ],
    );
    let bash = prompt
        .find("Check the [exit code: N] marker")
        .expect("bash guidance");
    let read = prompt.find("Use the read tool").expect("read guidance");
    assert!(bash < read, "tool guidance must follow tool name order");
    assert!(!prompt.contains("unknown"));
}

#[test]
fn every_implemented_tool_has_guidance() {
    for name in [
        "read",
        "write",
        "edit",
        "apply_patch",
        "glob",
        "grep",
        "bash",
        "pwsh",
    ] {
        assert!(tool_guidance(name).is_some(), "missing guidance for {name}");
    }
    assert!(tool_guidance("cordis_inspect").is_none());
}
