//! Entry point for the bench binary. Module root at src/ allows imports
//! from any subdirectory (usagelog/, tools/, etc.).
pub const main = @import("tools/bench.zig").main;
