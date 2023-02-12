use std::{
    io::{self, Stdout, Write},
    sync::Arc,
    thread,
    time::Duration,
};

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::{FutureExt, StreamExt};
use log::{error, info};
use pretty_env_logger::env_logger::{Builder, Env};
use rmk_daemon::{shutdown::shutdown_manager, state::RmkDaemon};
use tokio::{
    runtime::Handle,
    sync::{
        mpsc::{self, Sender, UnboundedSender},
        RwLock,
    },
};
use tui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::{Block, Borders},
    Frame, Terminal,
};
use tui_logger::{TuiLoggerLevelOutput, TuiLoggerSmartWidget, TuiLoggerWidget};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Builder::from_env(Env::new().default_filter_or("info")).init();
    tui_logger::init_logger(log::LevelFilter::Info)?;

    let daemon = RmkDaemon::try_new().await?;

    let handle = Handle::current();

    let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();
    let (send, mut recv) = mpsc::channel(1);

    handle.spawn(run_app(shutdown_send, send.clone()));

    drop(send);

    handle
        .spawn(shutdown_manager(shutdown_recv, async move {
            let daemon = daemon.clone();

            info!("Waiting for shutdown to complete...");
            let _ = recv.recv().await;

            daemon.stop()
        }))
        .await??;

    info!("Exiting...");

    Ok(())
}

async fn run_app(shutdown_send: UnboundedSender<()>, _sender: Sender<()>) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut reader = EventStream::new();

    loop {
        terminal.draw(|f| ui(f))?;
        let mut event = reader.next().fuse();

        tokio::select! {

            maybe_event = event => match maybe_event {
                Some(Ok(Event::Key(key))) => match key.code {
                    KeyCode::Char('q') => {
                        shutdown_send.send(())?;
                        break;
                    }
                    _ => info!("Key: {:?}", key)
                },
                Some(Err(err)) => error!("Error reading event: {}", err),
                None => break,
                _ => {}
            }
        }
    }

    disable_raw_mode()?;

    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;

    terminal.show_cursor()?;

    info!("Restored terminal");

    Ok(())
}

fn ui<B: Backend>(f: &mut Frame<B>) {
    let size = f.size();
    let block = Block::default().title("Block").borders(Borders::ALL);
    let inner_area = block.inner(size);
    f.render_widget(block, size);

    let mut constraints = vec![
        Constraint::Length(3),
        Constraint::Percentage(50),
        Constraint::Min(3),
    ];

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(inner_area);

    let tui_w: TuiLoggerWidget = TuiLoggerWidget::default()
        .block(
            Block::default()
                .title("Logs")
                .border_style(Style::default().fg(Color::White).bg(Color::Black))
                .borders(Borders::ALL),
        )
        .style_error(Style::default().fg(Color::Red))
        .style_debug(Style::default().fg(Color::Green))
        .style_warn(Style::default().fg(Color::Yellow))
        .style_trace(Style::default().fg(Color::Magenta))
        .style_info(Style::default().fg(Color::Cyan))
        .output_separator('|')
        .output_timestamp(Some("%F %H:%M:%S%.3f".to_string()))
        .output_level(Some(TuiLoggerLevelOutput::Long))
        .output_target(false)
        .output_file(false)
        .output_line(false)
        .style(Style::default().fg(Color::White).bg(Color::Black));
    f.render_widget(tui_w, chunks[2]);
}
