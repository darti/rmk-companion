use std::io;

use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::{FutureExt, StreamExt};
use log::{error, info};

use tokio::sync::mpsc::{Sender, UnboundedSender};
use tui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Row, Table, TableState},
    Frame, Terminal,
};
use tui_logger::TuiLoggerWidget;

use tui_textarea::TextArea;
#[derive(Default)]
pub struct App<'a> {
    query: TextArea<'a>,
    table_state: TableState,
}

pub async fn run_app<'a>(
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
                .title("Query"),
        );

        f.render_widget(app.query.widget(), chunks[0]);

        let selected_style = Style::default().add_modifier(Modifier::REVERSED);

        let headers = Row::new(vec!["Header1", "Header2", "Header3"]).style(
            Style::default()
                .add_modifier(Modifier::BOLD)
                .add_modifier(Modifier::UNDERLINED),
        );

        let table = Table::new([
            Row::new(vec!["Cell1", "Cell2", "Cell3"]),
            Row::new(vec!["Cell4", "Cell5", "Cell6"]),
        ])
        .header(headers)
        .style(Style::default().fg(Color::White).bg(Color::Black))
        .highlight_style(selected_style)
        .highlight_symbol(">> ")
        .widths(&[
            Constraint::Percentage(30),
            Constraint::Percentage(30),
            Constraint::Percentage(30),
        ])
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
