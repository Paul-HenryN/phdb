//! Our system uses a single binary file to store an entire database
//! This file in an unordered collection of fixed-size blocks of data called *pages*
//! A page stores an entity of our database (table, index, etc.)
//! This particular db file organization is commonly referred to as a heap file
//! Each page has a unique identifier #pageid. Since an entire db fits into a single heap file, we can precisely retrieve any page
//! by reading our file at an offset o = pageid * pagesize, assuming that all pages have the same size {pagesize}
//! SQlite uses this approach and keeps a special (sub)page at the top of the heap file to keep track of the page
//! organization as well as other metadata. See https://www.sqlite.org/fileformat.html

use std::{
    fs::{File, OpenOptions},
    io::{self},
    mem,
    os::unix::fs::FileExt,
};

type PageNumber = u32;
const MAGIC_NUMBER: u32 = 0x50484442;
const DEFAULT_PAGE_SIZE: u16 = 1024;

struct DbHeader {
    magic: u32,
    page_size: u16,
    page_count: u32,
}
impl DbHeader {
    fn to_buf(&self) -> [u8; mem::size_of::<Self>()] {
        let mut buf = [0_u8; mem::size_of::<Self>()];
        let mut offset = 0;

        buf[offset..mem::size_of_val(&self.magic) + offset]
            .copy_from_slice(&self.magic.to_le_bytes());
        offset += mem::size_of_val(&self.magic);

        buf[offset..mem::size_of_val(&self.page_size) + offset]
            .copy_from_slice(&self.page_size.to_le_bytes());
        offset += mem::size_of_val(&self.page_size);

        buf[offset..mem::size_of_val(&self.page_count) + offset]
            .copy_from_slice(&self.page_count.to_le_bytes());

        buf
    }

    fn from(buf: &[u8]) -> Self {
        let mut offset = 0;

        let magic = u32::from_le_bytes(
            buf[offset..mem::size_of::<u32>() + offset]
                .try_into()
                .expect("Invalid size"),
        );
        offset += mem::size_of::<u32>();

        let page_size = u16::from_le_bytes(
            buf[offset..mem::size_of::<u16>() + offset]
                .try_into()
                .expect("Invalid size"),
        );
        offset += mem::size_of::<u16>();

        let page_count = u32::from_le_bytes(
            buf[offset..mem::size_of::<u32>() + offset]
                .try_into()
                .expect("Invalid size"),
        );

        Self {
            magic,
            page_size,
            page_count,
        }
    }

    fn alloc(page_size: u16) -> Self {
        Self {
            magic: MAGIC_NUMBER,
            page_size,
            page_count: 0,
        }
    }
}

// This struct implements an in-memory cache representation of a database heap file. It reads and writes pages one at a time
// from and to disk.
#[derive(Debug)]
struct Pager {
    file: File,
    page_size: u16,
}
impl Pager {
    fn init(&mut self) -> io::Result<()> {
        let mut header = [0_u8; mem::size_of::<DbHeader>()];
        self.read(0, &mut header)?;
        let header = DbHeader::from(&header);

        if header.magic == MAGIC_NUMBER {
            self.page_size = header.page_size;
            return Ok(());
        }

        self.write(0, &DbHeader::alloc(self.page_size).to_buf())?;
        Ok(())
    }

    fn read(&self, page_no: PageNumber, buf: &mut [u8]) -> io::Result<usize> {
        self.file
            .read_at(buf, (page_no * self.page_size as u32).into())
    }

    fn write(&self, page_no: PageNumber, buf: &[u8]) -> io::Result<usize> {
        self.file
            .write_at(buf, (page_no * self.page_size as u32).into())
    }
}

fn main() -> io::Result<()> {
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .truncate(false)
        .write(true)
        .open("mydb.phdb")?;

    let mut pager = Pager {
        file,
        page_size: DEFAULT_PAGE_SIZE,
    };

    pager.init()?;

    println!("{:?}", pager);

    Ok(())
}
