import { Container, Spacer, Text } from '@mariozechner/pi-tui';
import packageJson from '../../package.json';
import { theme } from '../theme.js';
import { getModelDisplayName } from '../utils/model.js';

const INTRO_WIDTH = 60;

export class IntroComponent extends Container {
  private readonly modelText: Text;

  constructor(model: string) {
    super();

    const isDemo = process.env.KALSHI_USE_DEMO === 'true';
    const welcomeText = isDemo ? 'Kalshi Trading Bot CLI  [DEMO MODE]' : 'Kalshi Trading Bot CLI';
    const versionText = ` v${packageJson.version}`;
    const fullText = welcomeText + versionText;
    const padding = Math.max(0, Math.floor((INTRO_WIDTH - fullText.length - 2) / 2));
    const trailing = Math.max(0, INTRO_WIDTH - fullText.length - padding - 2);

    this.addChild(new Spacer(1));
    this.addChild(new Text(theme.primary('═'.repeat(INTRO_WIDTH)), 0, 0));
    this.addChild(
      new Text(
        theme.primary(
          `║${' '.repeat(padding)}${theme.bold(welcomeText)}${theme.muted(versionText)}${' '.repeat(
            trailing,
          )}║`,
        ),
        0,
        0,
      ),
    );
    this.addChild(new Text(theme.primary('═'.repeat(INTRO_WIDTH)), 0, 0));
    this.addChild(new Spacer(1));

    this.addChild(
      new Text(
        theme.bold(
          theme.primary(
            `
 ██████╗  ██████╗████████╗ █████╗  ██████╗  ██████╗ ███╗   ██╗
██╔═══██╗██╔════╝╚══██╔══╝██╔══██╗██╔════╝ ██╔═══██╗████╗  ██║
██║   ██║██║        ██║   ███████║██║  ███╗██║   ██║██╔██╗ ██║
██║   ██║██║        ██║   ██╔══██║██║   ██║██║   ██║██║╚██╗██║
╚██████╔╝╚██████╗   ██║   ██║  ██║╚██████╔╝╚██████╔╝██║ ╚████║
 ╚═════╝  ╚═════╝   ╚═╝   ╚═╝  ╚═╝ ╚═════╝  ╚═════╝ ╚═╝  ╚═══╝`,
          ),
        ),
        0,
        0,
      ),
    );

    if (isDemo) {
      this.addChild(new Spacer(1));
      this.addChild(
        new Text(
          theme.warning('  ⚠  DEMO MODE — orders are simulated, no real money at risk  ⚠'),
          0,
          0,
        ),
      );
    }

    this.addChild(new Spacer(1));
    this.addChild(new Text('AI-powered prediction market terminal.', 0, 0));
    this.addChild(new Spacer(1));
    const cmd = (label: string) => theme.muted(label.padEnd(11));
    this.addChild(new Text(cmd('/search') + 'Search events by theme, ticker, or free-text; /search edge for edge scan', 0, 0));
    this.addChild(new Text(cmd('/portfolio') + 'Overview, positions, orders, balance, status', 0, 0));
    this.addChild(new Text(cmd('/analyze') + '<ticker>  Full analysis: edge, research, Kelly sizing', 0, 0));
    this.addChild(new Text(cmd('/watch') + '<ticker>  Live price/orderbook feed', 0, 0));
    this.addChild(new Text(cmd('/backtest') + 'Model accuracy scorecard + live edge scanner', 0, 0));
    this.addChild(new Text(cmd('/buy /sell') + '<ticker> <n> [price]   /cancel <order_id>', 0, 0));
    this.addChild(new Text(cmd('/help') + '[command]  Show help (/help <command> for details)', 0, 0));
    this.addChild(new Text(cmd('/quit') + 'Quit CLI session', 0, 0));
    this.addChild(new Spacer(1));
    this.addChild(new Text(theme.muted('Ask anything: ') + '"analyze KXBTC"  "search crypto"  "show my portfolio"', 0, 0));
    this.modelText = new Text('', 0, 0);
    this.addChild(this.modelText);
    this.setModel(model);
  }

  setModel(model: string) {
    this.modelText.setText(
      `${theme.muted('Model: ')}${theme.primary(getModelDisplayName(model))}${theme.muted(
        '. Type /model to change.',
      )}`,
    );
  }
}
