use std::{
    fs::{File, OpenOptions},
    io::{self, Write},
    mem,
    os::unix::fs::FileExt,
};

#[derive(Debug)]
struct Pager {
    file: File,
    page_size: u32,
}

#[derive(Debug)]
struct DbHeader {
    magic_number: u32,
    page_size: u32,
    total_pages: u32,
    first_free_page: u32,
    last_free_page: u32,
}
impl DbHeader {
    fn to_buffer(&self) -> [u8; mem::size_of::<Self>()] {
        let mut buf = [0_u8; mem::size_of::<Self>()];

        buf[..4].copy_from_slice(&self.magic_number.to_le_bytes());
        buf[4..8].copy_from_slice(&self.page_size.to_le_bytes());
        buf[8..12].copy_from_slice(&self.total_pages.to_le_bytes());
        buf[12..16].copy_from_slice(&self.first_free_page.to_le_bytes());
        buf[16..20].copy_from_slice(&self.last_free_page.to_le_bytes());

        buf
    }

    fn from_buffer(buf: &[u8; mem::size_of::<Self>()]) -> Self {
        Self {
            magic_number: u32::from_le_bytes(buf[..4].try_into().expect("Incorrect size")),
            page_size: u32::from_le_bytes(buf[4..8].try_into().expect("Incorrect size")),
            total_pages: u32::from_le_bytes(buf[8..12].try_into().expect("Incorrect size")),
            first_free_page: u32::from_le_bytes(buf[12..16].try_into().expect("Incorrect size")),
            last_free_page: u32::from_le_bytes(buf[16..20].try_into().expect("Incorrect size")),
        }
    }
}

struct Page {
    content: String,
}
impl Page {
    fn to_buffer(&self) -> &[u8] {
        self.content.as_bytes()
    }
}

const DEFAULT_PAGE_SIZE: u32 = 512;
const MAGIC: u32 = 0x50484442;

impl Pager {
    fn init(&mut self) -> io::Result<()> {
        let mut header_buf = [0_u8; mem::size_of::<DbHeader>()];
        self.read(0, &mut header_buf)?;

        let header = DbHeader::from_buffer(&header_buf);

        if header.magic_number == MAGIC {
            self.page_size = header.page_size;
            return Ok(());
        }

        let header = DbHeader {
            magic_number: MAGIC,
            page_size: self.page_size,
            total_pages: 1,
            first_free_page: 0,
            last_free_page: 0,
        };

        let read: usize = self.file.write(&header.to_buffer())?;

        Ok(())
    }

    fn read(&mut self, page_number: u32, buf: &mut [u8]) -> io::Result<usize> {
        if page_number == 0 {
            return self.file.read_at(buf, 0);
        }

        let offset = mem::size_of::<DbHeader>() as u32 + self.page_size * (page_number - 1);
        self.file.read_at(buf, offset as u64)
    }

    fn write(&mut self, page_number: u32, buf: &[u8]) -> io::Result<usize> {
        let offset = mem::size_of::<DbHeader>() as u32 + self.page_size * (page_number - 1);
        self.file.write_at(buf, offset as u64)
    }
}

#[derive(Debug)]
struct Database {
    name: String,
    pager: Pager,
}

impl Database {
    fn init(name: &str) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(name.to_owned() + ".phdb")?;

        let mut pager = Pager {
            file,
            page_size: DEFAULT_PAGE_SIZE,
        };

        pager.init()?;

        Ok(Self {
            name: name.to_string(),
            pager,
        })
    }
}

fn main() {
    let mut users_db = Database::init("products").unwrap();

    users_db
        .pager
        .write(2, b"Hello Page 2")
        .expect("Something went wrong");
}
