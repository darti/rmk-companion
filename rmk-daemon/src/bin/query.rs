use std::io::{self};

use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::{FutureExt, StreamExt};
use log::{error, info};

use rmk_daemon::{shutdown::shutdown_manager, state::RmkDaemon};
use tokio::{
    runtime::Handle,
    sync::mpsc::{self, Sender, UnboundedSender},
};
use tui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    style::{Color, Style},
    widgets::{Block, Borders, Row, Table, TableState},
    Frame, Terminal,
};
use tui_logger::TuiLoggerWidget;

use tui_textarea::{Input, TextArea};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Builder::from_env(Env::new().default_filter_or("info")).init();
    tui_logger::init_logger(log::LevelFilter::Info)?;

    let daemon = RmkDaemon::try_new().await?;

    let handle = Handle::current();

    let (shutdown_send, shutdown_recv) = mpsc::unbounded_channel();
    let (send, mut recv) = mpsc::channel(1);

    let app = App::default();

    handle.spawn(run_app(shutdown_send, send.clone(), app));

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

#[derive(Default)]
struct App<'a> {
    query: TextArea<'a>,
    table_state: TableState,
}

async fn run_app<'a>(
    shutdown_send: UnboundedSender<()>,
    _sender: Sender<()>,
    mut app: App<'a>,
) -> anyhow::Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut reader = EventStream::new();

    loop {
        terminal.draw(|f| ui(f, &mut app))?;
        let event = reader.next().fuse();

        tokio::select! {

            maybe_event = event => match maybe_event {
                Some(Ok(Event::Key(key))) if key.code == KeyCode::Esc => {
                    break;

                },
                Some(Ok(Event::Key(key)))=> {
                    app.query.input(key);

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

fn ui<B: Backend>(f: &mut Frame<B>, app: &mut App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(70), Constraint::Min(3)])
        .split(f.size());

    {
        let chunks = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
            .split(chunks[0]);

        app.query.set_block(
            Block::default()
                .style(Style::default().fg(Color::White).bg(Color::Black))
                .borders(Borders::ALL)
                .title("Result"),
        );

        f.render_widget(app.query.widget(), chunks[0]);

        let table = Table::new([
            Row::new(vec!["Cell1", "Cell2", "Cell3"]),
            Row::new(vec!["Cell4", "Cell5", "Cell6"]),
        ])
        .header(Row::new(vec!["Header1", "Header2", "Header3"]))
        .style(Style::default().fg(Color::White).bg(Color::Black))
        .block(Block::default().borders(Borders::ALL).title("Result"));

        f.render_stateful_widget(table, chunks[1], &mut app.table_state);
    }

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
        .style(Style::default().fg(Color::White).bg(Color::Black));

    f.render_widget(tui_w, chunks[1]);
}
