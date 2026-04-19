//! Entry-point wrapper so that @import paths in simulator/ resolve from src/.
//! The real implementation lives in simulator/vopr.zig.
pub const main = @import("simulator/vopr.zig").main;
