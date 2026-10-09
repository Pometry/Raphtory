#[cfg(test)]
mod test {
    use raphtory::errors::GraphError;
    use std::io;

    #[test]
    fn test_location_capture() {
        fn inner() -> Result<(), GraphError> {
            Err(io::Error::other(GraphError::IllegalSet("hi".to_string())))?;
            Ok(())
        }

        let res = inner().err().unwrap();
        println!("{}", res);
    }
}
