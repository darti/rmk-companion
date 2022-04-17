use crate::Error;
use crate::Result;

#[derive(Debug, Default)]
pub struct LinesData {
    pub version: i32,
    pub pages: Vec<Page>,
}

#[derive(Default, Debug)]
pub struct Page {
    pub layers: Vec<Layer>,
}

#[derive(Default, Debug)]
pub struct Layer {
    pub lines: Vec<Line>,
}

#[derive(Debug)]
pub enum BrushType {
    BallPoint,
    Marker,
    Fineliner,
    SharpPencil,
    TiltPencil,
    Brush,
    Highlighter,
    Eraser,
    EraseArea,
    EraseAll,
    Calligraphy,
    Pen,
    SelectionBrush,
}

impl Default for BrushType {
    fn default() -> BrushType {
        BrushType::Fineliner
    }
}

impl std::convert::TryFrom<i32> for BrushType {
    type Error = Error;

    fn try_from(value: i32) -> Result<Self> {
        match value {
            // There seem to be different "versions" of similar brushes (e.g.
            // "Brush" at 0 and 12). v3 seems e.g. to use Brush 0 while v5 seems
            // to use Brush 12.
            0 => Ok(BrushType::Brush),
            1 => Ok(BrushType::TiltPencil),
            2 => Ok(BrushType::Pen),
            3 => Ok(BrushType::Marker),
            4 => Ok(BrushType::Fineliner),
            5 => Ok(BrushType::Highlighter),
            6 => Ok(BrushType::Eraser),
            7 => Ok(BrushType::SharpPencil),
            8 => Ok(BrushType::EraseArea),
            9 => Ok(BrushType::EraseAll),
            10 => Ok(BrushType::SelectionBrush),
            11 => Ok(BrushType::SelectionBrush),
            12 => Ok(BrushType::Brush),
            13 => Ok(BrushType::SharpPencil),
            14 => Ok(BrushType::TiltPencil),
            15 => Ok(BrushType::BallPoint),
            16 => Ok(BrushType::Marker),
            17 => Ok(BrushType::Fineliner),
            18 => Ok(BrushType::Highlighter),
            21 => Ok(BrushType::Calligraphy),
            v => Err(Error::UnknownBrush(v)),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Color {
    Black,
    Grey,
    White,
    Blue,
    Red,
}

impl TryFrom<i32> for Color {
    type Error = Error;

    fn try_from(color_i: i32) -> Result<Self> {
        match color_i {
            0 => Ok(Color::Black),
            1 => Ok(Color::Grey),
            2 => Ok(Color::White),
            6 => Ok(Color::Blue),
            7 => Ok(Color::Red),
            _ => Err(Error::UnknownColor(color_i)),
        }
    }
}

impl Default for Color {
    fn default() -> Color {
        Color::Black
    }
}

#[derive(Default, Debug)]
pub struct Line {
    pub brush_type: BrushType,
    pub color: Color,
    pub unknown_line_attribute: i32,
    pub unknown_line_attribute_2: i32,
    pub brush_base_size: f32,
    pub points: Vec<Point>,
}

#[derive(Default, Debug, Clone)]
pub struct Point {
    pub x: f32,
    pub y: f32,
    pub speed: f32,
    pub direction: f32,
    pub width: f32,
    pub pressure: f32,
}

#[derive(Clone)]
pub struct LayerColor {
    pub black: String,
    pub grey: String,
    pub white: String,
    pub blue: String,
    pub red: String,
}

impl Default for LayerColor {
    fn default() -> Self {
        Self {
            black: "black".to_string(),
            grey: "#bfbfbf".to_string(),
            white: "white".to_string(),
            blue: "#0062cc".to_string(),
            red: "#d90707".to_string(),
        }
    }
}
