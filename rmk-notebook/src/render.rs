use std::io::Write;

use lopdf::{
    content::{Content, Operation},
    dictionary, Document, Object, Stream,
};

use crate::{rm::Line, Notebook, Result};

impl Notebook {
    pub fn render<W: Write>(&self, target: &mut W) -> Result<()> {
        // https://blog.idrsolutions.com/2010/11/grow-your-own-pdf-file-–-part-5-path-objects/

        // 1 pt = 1/72 inch
        // 1 inch = 2.54 cm
        let mm_to_pt = |mm: f64| mm * 72_f64 / 25.4_f64;

        let paper_width = 210.;
        let paper_height = 297.;

        let remarkable_width = 1404.;
        let remarkable_height = 1872.;

        let pdf_width = mm_to_pt(paper_width);
        let pdf_height = mm_to_pt(paper_height);

        let remarkable_pdf_ratio =
            (pdf_width / remarkable_width).min(pdf_height / remarkable_height);

        let media_box: Object =
            vec![0.into(), 0.into(), pdf_width.into(), pdf_height.into()].into();

        let transform = vec![
            Object::Real(remarkable_pdf_ratio),
            Object::Real(0.0),
            Object::Real(0.0),
            Object::Real(-remarkable_pdf_ratio),
            Object::Real(0.0),
            Object::Real(pdf_height),
        ];

        let mut doc = Document::with_version("1.5");
        let pages_id = doc.new_object_id();

        let mut page_ids: Vec<Object> = Vec::with_capacity(self.pages.len());

        for page in &self.pages {
            let mut content = Content { operations: vec![] };
            content
                .operations
                .push(Operation::new("cm", transform.clone()));

            for layer in &page.layers {
                for line in &layer.lines {
                    self.render_line(line, &mut content.operations);
                }
            }

            let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));

            page_ids.push(
                doc.add_object(dictionary! {
                    "Type" => "Page",
                    "Parent" => pages_id,
                    "Contents" => content_id,
                })
                .into(),
            );
        }

        let font_id = doc.add_object(dictionary! {
            "Type" => "Font",
            "Subtype" => "Type1",
            "BaseFont" => "Courier",
        });

        let resources_id = doc.add_object(dictionary! {
            "Font" => dictionary! {
                "F1" => font_id,
            },
        });

        let pages = dictionary! {
            "Type" => "Pages",
            "Kids" => Object::Array(page_ids),
            "Count" => Object::Integer(self.pages.len() as i64),
            "Resources" => resources_id,
            "MediaBox" => media_box,

        };

        doc.objects.insert(pages_id, Object::Dictionary(pages));
        let catalog_id = doc.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });

        doc.trailer.set("Root", catalog_id);
        // doc.compress();
        doc.save_to(target)?;

        Ok(())
    }

    fn render_line(&self, line: &Line, operations: &mut Vec<Operation>) -> Result<()> {
        let mut points = line.points.iter();
        let origin = points.next().unwrap();

        operations.push(Operation::new("m", vec![origin.x.into(), origin.y.into()]));

        points.for_each(|pt| {
            operations.push(Operation::new("l", vec![pt.x.into(), pt.y.into()]));
        });

        // operations.push(Operation::new("h", vec![]));
        operations.push(Operation::new("S", vec![]));

        Ok(())
    }
}
