use bytes::{BufMut, BytesMut};
use std::fs::File;
use std::io::Result;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};

const PAGE_SIZE: usize = 4096;

pub struct FileHandle {
    num_pages: usize,
    file: File,
    read_count: usize,
    write_count: usize,
    append_count: usize,
}

impl FileHandle {
    pub fn new(mut file: File) -> std::result::Result<Self, std::io::Error> {
        let f_size = file.metadata()?.len();

        let (read_count, write_count, append_count, num_pages) = if f_size > 0 {
            //read in counters
            let mut counters_str = String::new();
            file.read_to_string(&mut counters_str)?;
            let split = counters_str.split("|");
            let counters: Vec<usize> = split
                .map(|s| s.trim())
                .filter(|s| !s.is_empty() && !s.starts_with("0"))
                // Should panic here, if we cant parse data we cant proceed
                .map(|s| s.parse().unwrap())
                .collect();
            if counters.len() != 4 {
                (0, 0, 0, 0)
            } else {
                // Safe because length is guaranteed to be 4
                (
                    *counters.get(0).unwrap(),
                    *counters.get(1).unwrap(),
                    *counters.get(2).unwrap(),
                    *counters.get(3).unwrap(),
                )
            }
        } else {
            let counters_str = format!("{}|{}|{}|{}", 0, 0, 0, 0);
            file.write(&counters_str.as_bytes())?;
            (0, 0, 0, 0)
        };
        Ok(Self {
            num_pages,
            file,
            write_count,
            read_count,
            append_count,
        })
    }

    fn write_counters(&mut self) -> Result<usize> {
        let Self {
            ref read_count,
            ref write_count,
            ref append_count,
            ref num_pages,
            ..
        } = self;
        let counters_str = format!(
            "{}|{}|{}|{}",
            read_count, write_count, append_count, num_pages
        );
        self.file.write(&counters_str.as_bytes())
    }

    pub fn read_page(&mut self, page_num: usize, data: &mut BytesMut) -> Result<usize> {
        if page_num >= self.num_pages {
            Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "Index Out of Bounds",
            ))
        } else {
            self.read_from(((page_num + 1) * PAGE_SIZE) as u64, data)
        }
    }

    // TODO: figure out why we have to write into buf, then data...
    fn read_from(&mut self, pos: u64, data: &mut BytesMut) -> Result<usize> {
        self.file.seek(SeekFrom::Start(pos))?;
        let mut buf: [u8; PAGE_SIZE] = [0; PAGE_SIZE];
        let bytes_read = self.file.read(&mut buf)?;
        data.put_slice(&buf);
        Ok(bytes_read)
    }

    pub fn write_page(&mut self, page_num: usize, data: &BytesMut) -> Result<usize> {
        if page_num >= self.num_pages {
            Err(std::io::Error::new(
                ErrorKind::InvalidInput,
                "Index Out of Bounds",
            ))
        } else {
            let bytes_written = self.write_to(((page_num + 1) * PAGE_SIZE) as u64, data)?;
            self.write_count += 1;
            Ok(bytes_written)
        }
    }
    fn write_to(&mut self, pos: u64, data: &BytesMut) -> Result<usize> {
        self.file.seek(SeekFrom::Start(pos))?;
        self.file.write(&data)
    }

    pub fn append_page(&mut self, data: &BytesMut) -> Result<usize> {
        let bytes_written = self.write_to(((self.num_pages + 1) * PAGE_SIZE) as u64, data)?;
        self.num_pages += 1;
        self.append_count += 1;
        Ok(bytes_written)
    }

    pub fn get_num_pages(&self) -> usize {
        self.num_pages
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use std::fs::OpenOptions;

    #[test]
    fn read_write_to_new_file() {
        let file_name = "./test_files/read_write_to_new_file";
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(file_name)
            .unwrap();

        let mut file_handle_res = FileHandle::new(file);
        assert!(file_handle_res.is_ok());
        let mut file_handle = file_handle_res.unwrap();

        let mut page = BytesMut::with_capacity(PAGE_SIZE);
        const DATA: &[u8] = b"Test Data";
        page.put(&DATA[..]);
        page.put(&[0; PAGE_SIZE - DATA.len()][..]);

        let mut res = file_handle.append_page(&page);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), PAGE_SIZE);

        let mut buf: BytesMut = BytesMut::with_capacity(PAGE_SIZE);
        res = file_handle.read_page(0, &mut buf);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), PAGE_SIZE);

        assert_eq!(page, buf);
        std::fs::remove_file(file_name);
    }

    #[test]
    fn read_write_multiple_pages() {
        let file_name = "./test_files/read_write_multiple_pages";
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(file_name)
            .unwrap();

        // Create file handle
        let mut file_handle_res = FileHandle::new(file);
        assert!(file_handle_res.is_ok());
        let mut file_handle = file_handle_res.unwrap();

        // Create first page
        let mut page = BytesMut::with_capacity(PAGE_SIZE);
        const DATA: &[u8] = b"Test Data on page 1";
        page.put(&DATA[..]);
        page.put(&[0; PAGE_SIZE - DATA.len()][..]);

        // Append it
        let mut res = file_handle.append_page(&page);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), PAGE_SIZE);

        // Create output buffer and read first page
        let mut buf: BytesMut = BytesMut::with_capacity(PAGE_SIZE);
        res = file_handle.read_page(0, &mut buf);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), PAGE_SIZE);

        // Check that the data we get back is the same
        assert_eq!(page, buf);

        // New page
        page.clear();
        const DATA2: &[u8] = b"New data for page 1";
        page.put(&DATA2[..]);
        page.put(&[0; PAGE_SIZE - DATA2.len()][..]);

        // Write it
        res = file_handle.write_page(0, &page);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), PAGE_SIZE);

        buf.clear();
        res = file_handle.read_page(0, &mut buf);
        assert!(res.is_ok());
        assert_eq!(res.unwrap(), PAGE_SIZE);

        assert_eq!(page, buf);
        std::fs::remove_file(file_name);
    }
}
