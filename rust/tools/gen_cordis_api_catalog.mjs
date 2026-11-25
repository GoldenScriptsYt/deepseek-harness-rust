import { writeFileSync } from 'node:fs'
import { EVENT_API, INHERITED_CTX_API, SERVICE_API, TYPE_API } from '../../packages/extensions/tool-cordis/src/api-catalog.ts'

const catalog = {
  generatedFrom: 'packages/extensions/tool-cordis/src/api-catalog.ts',
  services: SERVICE_API,
  events: EVENT_API,
  types: TYPE_API,
  inherited: INHERITED_CTX_API,
}

writeFileSync(
  new URL('../crates/dsh-tools/assets/cordis-api-catalog.json', import.meta.url),
  `${JSON.stringify(catalog)}\n`,
)
