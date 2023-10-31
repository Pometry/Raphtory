/// Splits the input text in chunks of no more than max_size trying to use line breaks
/// as much as possible
pub fn split_text_by_line_breaks(text: String, max_size: usize) -> Vec<String> {
    if text.len() <= max_size {
        vec![text]
    } else {
        // TODO: maybe use async_stream crate instead
        let mut substrings = text.split("\n");
        let first_substring = substrings.next().unwrap().to_owned();
        let mut chunks = vec![first_substring];

        for substring in substrings {
            let last_chunk = chunks.last_mut().unwrap(); // at least one element
            if substring.len() > max_size {
                for subsubstring in split_text_with_constant_size(substring, max_size).into_iter() {
                    chunks.push(subsubstring.to_owned());
                }
            } else if last_chunk.len() + substring.len() <= max_size {
                last_chunk.push_str("\n"); // add back line break removed by split
                last_chunk.push_str(substring);
            } else {
                chunks.push(substring.to_owned());
            }
        }

        chunks
    }
}

// TODO: test this function
pub fn split_text_with_constant_size(input: &str, chunk_size: usize) -> Vec<&str> {
    let mut substrings = Vec::new();
    let mut start = 0;

    while start < input.len() {
        // Use char_indices to ensure we split the string at valid UTF-8 boundaries
        let end = input[start..]
            .char_indices()
            .take(chunk_size)
            .map(|(i, _)| i)
            .last()
            .map_or(start + chunk_size, |idx| start + idx);

        let substring = &input[start..end];
        substrings.push(substring);

        start = end;
    }

    substrings
}
