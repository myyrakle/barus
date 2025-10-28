use crate::disktable::segment::segment_id::TableSegmentID;

// Table Segment State per Table
#[derive(Debug, Clone, Default)]
pub struct TableSegmentState {
    pub(crate) last_segment_id: TableSegmentID,
    pub(crate) segment_file_size: u32,
    pub(crate) current_page_offset: u32, // real offset in segment file
    pub(crate) current_page_index: u32,  // current page number in segment file (0-based index)
}
