/// connect outpoints of inputs to previous outputs
pub mod connected_proto;

/// add block hash, transaction id, script type, addresses to original `bitcoin::Block`
pub mod full_proto;

/// simplified blockchain objects, for faster python processing
pub mod simple_proto;
