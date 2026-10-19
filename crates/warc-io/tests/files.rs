use futures_util::io::AsyncReadExt;
use warc_io::opendal::{services::Memory, Operator};
use warc_io::WarcFile;

#[tokio::test]
async fn local_and_file_url_inputs_reopen_without_creating_input_directories() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("an archive.warc");
    std::fs::write(&path, b"first second").unwrap();
    for uri in [
        path.to_string_lossy().into_owned(),
        url::Url::from_file_path(&path).unwrap().to_string(),
    ] {
        let file = WarcFile::from_uri(&uri).unwrap();
        let mut prefix = [0; 5];
        file.open()
            .await
            .unwrap()
            .read_exact(&mut prefix)
            .await
            .unwrap();
        assert_eq!(&prefix, b"first");
        let mut complete = String::new();
        file.open()
            .await
            .unwrap()
            .read_to_string(&mut complete)
            .await
            .unwrap();
        assert_eq!(complete, "first second");
    }
    let missing_parent = directory.path().join("missing");
    let file = WarcFile::from_uri(missing_parent.join("a.warc").to_str().unwrap()).unwrap();
    assert!(!missing_parent.exists());
    let mut reader = file.open().await.unwrap();
    assert!(reader.read_to_end(&mut Vec::new()).await.is_err());
    assert!(!missing_parent.exists());
}

#[tokio::test]
async fn caller_supplied_operator_is_reusable() {
    let operator = Operator::new(Memory::default()).unwrap();
    operator.write("a.warc", "memory data").await.unwrap();
    let file = WarcFile::new(operator, "a.warc").unwrap();
    for _ in 0..2 {
        let mut bytes = Vec::new();
        file.open()
            .await
            .unwrap()
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        assert_eq!(bytes, b"memory data");
    }
    assert!(WarcFile::from_uri("").is_err());
}
