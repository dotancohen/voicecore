//! The levels a waveform is drawn from (FILE-20).
//!
//! Decoding a recording to draw its waveform is slow on a phone and fast on a
//! desktop, so whichever device decodes a recording first keeps the levels
//! with it: one loudness value per share of the recording, from 0 to 255, the
//! loudest share at 255. They are synced with the recording, and any device
//! draws any number of bars from them without reading the audio again.

use base64::Engine;

/// The most levels a recording keeps: the drawing code keeps eight per bar
/// for 150 bars (1,200), so this leaves room and bounds a row.
pub const LEVELS_MAX: usize = 4096;

/// The levels as the row keeps them: base64url without padding. None when
/// there are none or too many.
pub fn encode_levels(levels: &[u8]) -> Option<String> {
    if levels.is_empty() || levels.len() > LEVELS_MAX {
        return None;
    }
    Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(levels))
}

/// The levels from the row's text; None when it is not a set of levels.
pub fn decode_levels(text: &str) -> Option<Vec<u8>> {
    let levels = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(text.as_bytes()).ok()?;
    (!levels.is_empty() && levels.len() <= LEVELS_MAX).then_some(levels)
}

/// `bar_count` bars from the levels, each the loudest level of its share,
/// scaled so the loudest bar fills the height; as many bars as levels when
/// there are fewer levels than bars. The same bars the desktop and the phone
/// draw when they decode the recording themselves.
pub fn bars_from_levels(levels: &[u8], bar_count: usize) -> Vec<f32> {
    if levels.is_empty() || bar_count == 0 {
        return Vec::new();
    }
    let loudest = f32::from(*levels.iter().max().expect("not empty"));
    let scaled = |value: u8| if loudest > 0.0 { f32::from(value) / loudest } else { 0.0 };
    if levels.len() <= bar_count {
        return levels.iter().map(|v| scaled(*v)).collect();
    }
    (0..bar_count)
        .map(|bar| {
            let from = bar * levels.len() / bar_count;
            let to = ((bar + 1) * levels.len() / bar_count).max(from + 1).min(levels.len());
            scaled(*levels[from..to].iter().max().expect("a share is never empty"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FILE-20: bars are the loudest of each share, scaled to the loudest bar.
    #[test]
    fn bars_are_the_loudest_level_of_each_share_scaled_to_the_loudest() {
        let levels: Vec<u8> = (0..1200).map(|i| if i == 700 { 255 } else { (i % 100) as u8 }).collect();
        let bars = bars_from_levels(&levels, 150);
        assert_eq!(bars.len(), 150);
        assert_eq!(bars[700 * 150 / 1200], 1.0, "the share holding the loudest level fills the height");
        assert!(bars.iter().all(|b| (0.0..=1.0).contains(b)));
        assert_eq!(bars_from_levels(&[0, 51, 102], 150), vec![0.0, 0.5, 1.0], "fewer levels than bars: one bar each");
        assert!(bars_from_levels(&[], 150).is_empty());
        assert_eq!(bars_from_levels(&[0, 0, 0], 2), vec![0.0, 0.0], "silence is flat, not a division by zero");
    }

    /// FILE-20: the levels round-trip through the row's text, within bounds.
    #[test]
    fn levels_round_trip_through_text_and_too_many_are_refused() {
        let levels: Vec<u8> = (0..1200).map(|i| (i % 256) as u8).collect();
        assert_eq!(decode_levels(&encode_levels(&levels).unwrap()).unwrap(), levels);
        assert!(encode_levels(&[]).is_none());
        assert!(encode_levels(&vec![1u8; LEVELS_MAX + 1]).is_none());
        assert!(decode_levels("not levels!").is_none());
    }
}
