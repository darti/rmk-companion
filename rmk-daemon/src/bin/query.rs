use std::{io, thread, time::Duration};

use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use pretty_env_logger::env_logger::{Builder, Env};
use rmk_daemon::{shutdown::shutdown_manager, state::RmkDaemon};
use tokio::runtime::Handle;
use tui::{
    backend::CrosstermBackend,
    widgets::{Block, Borders},
    Terminal,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    Builder::from_env(Env::new().default_filter_or("info")).init();

    let daemon = RmkDaemon::try_new().await?;

    let handle = Handle::current();

    handle.spawn(shutdown_manager(async move {
        let daemon = daemon.clone();
        daemon.stop()
    }));

    // setup terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    terminal.draw(|f| {
        let size = f.size();
        let block = Block::default().title("Block").borders(Borders::ALL);
        f.render_widget(block, size);
    })?;

    thread::sleep(Duration::from_millis(5000));

    // restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    Ok(())
}
