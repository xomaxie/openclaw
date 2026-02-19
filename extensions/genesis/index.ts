import type { AnyAgentTool, ChannelPlugin, OpenClawPluginApi } from 'openclaw/plugin-sdk';
import { emptyPluginConfigSchema } from 'openclaw/plugin-sdk';
import { genesisPlugin } from './src/channel.js';
import { genesisTools } from './src/tools.js';

const plugin = {
  id: 'genesis',
  name: 'GENESIS',
  description: 'GENESIS webchat bridge channel plugin',
  configSchema: emptyPluginConfigSchema(),
  register(api: OpenClawPluginApi) {
    api.registerChannel({ plugin: genesisPlugin as ChannelPlugin });
    // Tools for GENESIS task orchestration (create/dispatch/query).
    for (const t of genesisTools(api)) {
      api.registerTool(t as AnyAgentTool);
    }
  },
};

export default plugin;
