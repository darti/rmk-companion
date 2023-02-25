use lazy_static::lazy_static;

use crate::backend::table_static::RmkNode;

lazy_static! {
    pub static ref STATIC_FILES: [RmkNode<'static>; 6] = [
        RmkNode::new(".", "CollectionType", ".", None, None),
        RmkNode::new(".", "CollectionType", "..", None, None),
        RmkNode::new(
            ".VolumeIcon.icns",
            "DocumentType",
            ".VolumeIcon.icns",
            None,
            Some(include_bytes!("./resources/.VolumeIcon.icns")),
        ),
        RmkNode::new(
            "._.VolumeIcon.icns",
            "DocumentType",
            "._.VolumeIcon.icns",
            None,
            Some(include_bytes!("./resources/._.VolumeIcon.icns")),
        ),
        RmkNode::new(
            "._.",
            "DocumentType",
            "._.",
            None,
            Some(include_bytes!("./resources/._.")),
        ),
        RmkNode::new(
            "._.com.apple.timemachine.donotpresent",
            "DocumentType",
            "._.com.apple.timemachine.donotpresent",
            None,
            Some(include_bytes!(
                "./resources/._.com.apple.timemachine.donotpresent"
            )),
        ),
    ];
}
