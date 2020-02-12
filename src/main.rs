use crossbeam_channel::{bounded, select, Receiver};
use serde::{Deserialize, Serialize};
use std::convert::TryInto;
use std::ffi::OsString;
use std::fs;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::{thread, time};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(about = "tail equinox log files and relay entries")]
struct Opt {
    /// Verbose output
    #[structopt(short, long)]
    verbose: bool,

    /// Wait for more input instead of exiting
    #[structopt(short, long)]
    wait: bool,

    #[structopt(short, long)]
    /// Filename of state file
    state_file: Option<String>,

    #[structopt(subcommand)] // Note that we mark a field as a subcommand
    cmd: Command,
}

#[derive(StructOpt, Debug)]
enum OutputSpec {
    /// Output to a file
    File {
        /// Directory to process
        #[structopt(parse(from_os_str))]
        file: PathBuf,
    },
    /// No output
    None,
}

#[derive(StructOpt, Debug)]
enum Command {
    /// Process a directory (this is the "normal" situation)
    Dir {
        /// Directory to process
        #[structopt(parse(from_os_str))]
        dir: PathBuf,
        #[structopt(subcommand)] // Note that we mark a field as a subcommand
        output: OutputSpec,
    },
    /// Process a single file (usually for testing purposes)
    File {
        /// File to process
        #[structopt(parse(from_os_str))]
        file: PathBuf,
        #[structopt(subcommand)] // Note that we mark a field as a subcommand
        output: OutputSpec,
    },
}

fn main() {
    match do_main() {
        Ok(()) => {}
        Err(e) => {
            println!("{}", e.to_string());
            std::process::exit(-1);
        }
    }
}

fn do_main() -> io::Result<()> {
    let opt = Opt::from_args();

    let quit = ctrl_channel().unwrap();

    match opt.cmd {
        Command::File { file, output } => {
            let mut sr = SingleEntryReader::open(&file)?;
            process(
                &mut sr,
                &quit,
                opt.state_file,
                opt.wait,
                opt.verbose,
                output,
            )?;
        }
        Command::Dir { dir, output } => {
            let mut mr = MultiEntryReader::new(&dir, opt.verbose);
            process(
                &mut mr,
                &quit,
                opt.state_file,
                opt.wait,
                opt.verbose,
                output,
            )?;
        }
    }

    Ok(())
}

trait EntryReader {
    fn next(&mut self, buf: &mut Vec<u8>) -> io::Result<Option<u64>>;
    fn position(&self) -> String;
    fn seek(&mut self, pos: &str) -> io::Result<()>;
}

fn process(
    r: &mut dyn EntryReader,
    quit: &Receiver<()>,
    state_file_name: Option<String>,
    wait: bool,
    verbose: bool,
    os: OutputSpec,
) -> io::Result<()> {
    match &state_file_name {
        None => {
            do_process(r, quit, wait, verbose, os)?;
        }
        Some(state_file_name) => {
            match fs::File::open(state_file_name) {
                Ok(f) => {
                    let mut br = io::BufReader::new(f);
                    let mut s = String::new();
                    br.read_to_string(&mut s)?;

                    r.seek(&s)?;
                }
                Err(e) => {
                    if e.kind() != ErrorKind::NotFound {
                        return Err(e);
                    }
                }
            }
            do_process(r, quit, wait, verbose, os)?;
            let mut f = fs::File::create(state_file_name)?;
            write!(&mut f, "{}", &r.position())?;
        }
    }

    Ok(())
}

trait EntryWriter {
    fn write_entry(&mut self, buf: &[u8]) -> io::Result<()>;
}

struct FileEntryWriter {
    w: io::BufWriter<File>,
}

impl EntryWriter for FileEntryWriter {
    fn write_entry(&mut self, buf: &[u8]) -> io::Result<()> {
        self.w.write_all(buf)
    }
}

struct NoneEntryWriter {}

impl EntryWriter for NoneEntryWriter {
    fn write_entry(&mut self, _buf: &[u8]) -> io::Result<()> {
        Ok(())
    }
}

fn do_process(
    r: &mut dyn EntryReader,
    quit: &Receiver<()>,
    wait: bool,
    verbose: bool,
    os: OutputSpec,
) -> io::Result<()> {
    let mut out: Box<dyn EntryWriter>;
    match os {
        OutputSpec::None => {
            out = Box::new(NoneEntryWriter {});
        }
        OutputSpec::File { file } => {
            let so = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(file)?;
            let bw = io::BufWriter::new(so);
            out = Box::new(FileEntryWriter { w: bw });
        }
    }

    let mut buf = Vec::new();

    let mut c: u8 = 0;

    loop {
        if c == 255 {
            select! {
                recv(quit) -> _ => {
                    return Ok(());
                }
                default() => {}
            }
            c = 0;
        } else {
            c += 1;
        }

        match r.next(&mut buf)? {
            Some(_len) => (*out).write_entry(&buf)?,
            None => {
                if !wait {
                    return Ok(());
                } else {
                    if verbose {
                        println!("waiting for data.")
                    }
                    let second = time::Duration::from_millis(1000);
                    thread::sleep(second);
                }
            }
        }
        buf.clear();
    }
}

fn ctrl_channel() -> Result<Receiver<()>, ctrlc::Error> {
    let (sender, receiver) = bounded(100);
    ctrlc::set_handler(move || {
        let _ = sender.send(());
    })?;

    Ok(receiver)
}

struct SingleEntryReader {
    r: io::BufReader<File>,
    pos: u64,
}

// Read exactly count bytes into buf.  If an error occurs, buf may have had partial data added, and
// r may be been read from.
fn read_count(r: &mut impl BufRead, buf: &mut Vec<u8>, count: usize) -> io::Result<()> {
    let mut remaining = count;
    loop {
        let available = match r.fill_buf() {
            Ok(n) => n,
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        let al = available.len();
        if al == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"));
        }
        if al < remaining {
            buf.extend_from_slice(&available);
            remaining -= available.len();
            r.consume(al);
        } else {
            buf.extend_from_slice(&available[0..remaining]);
            r.consume(remaining);
            return Ok(());
        }
    }
}

impl EntryReader for SingleEntryReader {
    fn next(&mut self, buf: &mut Vec<u8>) -> io::Result<Option<u64>> {
        let cl = buf.len();

        // read the header to get the length of the entry
        match read_count(&mut self.r, buf, 5) {
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    buf.truncate(cl);
                    self.r.seek(SeekFrom::Start(self.pos)).expect("seek failed");
                    return Ok(None);
                }
            }
            Ok(()) => {}
        }

        let len: usize = u32::from_le_bytes(buf[cl + 1..cl + 5].try_into().unwrap()) as usize;

        // read the rest of the entry (the -5 is because we already read the header)
        match read_count(&mut self.r, buf, len - 5) {
            Err(e) => {
                if e.kind() == ErrorKind::UnexpectedEof {
                    buf.truncate(cl);
                    self.r.seek(SeekFrom::Start(self.pos)).expect("seek failed");
                    return Ok(None);
                }
            }
            Ok(()) => {}
        }

        let l = len as u64;
        self.pos += l;
        Ok(Some(l))
    }

    fn position(&self) -> String {
        return format!("{}", self.pos);
    }

    fn seek(&mut self, s: &str) -> io::Result<()> {
        self.pos = s.parse().expect("parse failed");
        self.r.seek(SeekFrom::Start(self.pos))?;
        Ok(())
    }
}

impl SingleEntryReader {
    fn open(name: &Path) -> io::Result<SingleEntryReader> {
        let f = File::open(name)?;
        let f = io::BufReader::with_capacity(32 * 1024, f);
        let sr = SingleEntryReader { r: f, pos: 0 };
        Ok(sr)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct FileLocation {
    dir: OsString,
    name: OsString,
}

impl FileLocation {
    fn full_name(&self, base: &Path) -> PathBuf {
        let mut p = PathBuf::from(base);
        p.push(&self.dir);
        p.push(&self.name);
        p
    }
}

impl Clone for FileLocation {
    fn clone(&self) -> Self {
        FileLocation {
            dir: self.dir.clone(),
            name: self.name.clone(),
        }
    }
}

struct MultiEntryReader<'a> {
    dir: &'a Path,

    f: Option<SingleEntryReader>,

    last: Option<FileLocation>,
    next: Option<FileLocation>,

    verbose: bool,
}

impl EntryReader for MultiEntryReader<'_> {
    fn next(&mut self, buf: &mut Vec<u8>) -> io::Result<Option<u64>> {
        loop {
            match &mut self.f {
                Some(f) => {
                    if let Some(r) = f.next(buf)? {
                        return Ok(Some(r));
                    }
                }
                None => {}
            }

            // now we either don't have a file, or we're at EOF

            // we only move on to the next file here if we already determined there is one last
            // time around the loop. Otherwise, we record that there is one and wait for the next
            // time around.  This is to avoid a race when we're at EOF, but between us finding that
            // EOF and checking for the next file, not only has the next file been created, but
            // more bytes were also written to the current file, so we can theoretically miss data
            // otherwise.
            match &self.next {
                Some(next) => {
                    let filename = next.full_name(self.dir);
                    if self.verbose {
                        println!("opening file {}.", &filename.to_str().unwrap())
                    }

                    let f = SingleEntryReader::open(&filename)?;
                    self.f = Some(f);

                    self.last = self.next.clone();
                    self.next = None;
                }
                None => {
                    // look for next file to open
                    match determine_next_location(self.dir, &self.last)? {
                        None => {
                            return Ok(None);
                        }
                        Some(nl) => {
                            self.next = Some(nl);
                            // do nothing & allow to loop.
                        }
                    }
                }
            }
        }
    }

    fn position(&self) -> String {
        let mut ms = MultiState {
            dir: "".to_string(),
            name: "".to_string(),
            file_pos: "".to_string(),
        };

        match &self.f {
            Some(f) => {
                ms.file_pos = f.position();
                match &self.last {
                    None => {}
                    Some(l) => {
                        ms.dir = l.dir.to_str().unwrap().to_string();
                        ms.name = l.name.to_str().unwrap().to_string();
                    }
                }
            }
            None => {}
        }

        serde_json::to_string(&ms).unwrap()
    }

    fn seek(&mut self, s: &str) -> io::Result<()> {
        let st: MultiState = serde_json::from_str(s)?;
        let fl = FileLocation {
            dir: OsString::from(&st.dir),
            name: OsString::from(&st.name),
        };
        let filename = fl.full_name(self.dir);
        self.last = Some(fl);
        self.next = None;

        if self.verbose {
            println!("opening file {}.", &filename.to_str().unwrap())
        }

        let mut f = SingleEntryReader::open(&filename)?;

        f.seek(&st.file_pos)?;

        self.f = Some(f);

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct MultiState {
    dir: String,
    name: String,
    file_pos: String,
}

impl MultiEntryReader<'_> {
    fn new(dir: &Path, verbose: bool) -> MultiEntryReader {
        MultiEntryReader {
            dir,
            f: None,
            last: None,
            next: None,
            verbose,
        }
    }
}

fn determine_next_location(
    dirname: &Path,
    last: &Option<FileLocation>,
) -> io::Result<Option<FileLocation>> {
    match last {
        Some(last_loc) => {
            let nd = next_entry(&dirname, &Some(last_loc.dir.clone()))?;

            let mut pb = PathBuf::new();
            pb.push(&dirname);
            pb.push(&last_loc.dir);

            if let Some(nf) = next_entry(&pb, &Some(last_loc.name.clone()))? {
                return Ok(Some(FileLocation {
                    dir: last_loc.dir.clone(),
                    name: nf,
                }));
            }

            if let Some(nd) = nd {
                let mut pb = PathBuf::new();
                pb.push(&dirname);
                pb.push(&nd);
                if let Some(nf) = next_entry(&pb, &None)? {
                    return Ok(Some(FileLocation { dir: nd, name: nf }));
                }
            }

            Ok(None)
        }
        None => {
            // no existing state, we're at the start.
            match next_entry(dirname, &None)? {
                None => Ok(None),
                Some(firstdir) => {
                    // look if we have a new file in the existing dir
                    let mut buf = PathBuf::new();
                    buf.push(dirname);
                    buf.push(firstdir.clone());

                    match next_entry(&buf, &None)? {
                        Some(next_file) => Ok(Some(FileLocation {
                            dir: firstdir,
                            name: next_file,
                        })),
                        None => Ok(None),
                    }
                }
            }
        }
    }
}

fn next_entry(parent_dir: &Path, current_entry: &Option<OsString>) -> io::Result<Option<OsString>> {
    let ce;
    match &current_entry {
        None => {
            ce = "<none>".to_string();
        }
        Some(curr) => {
            ce = curr.to_string_lossy().to_string();
        }
    }

    let x = do_next_entry(&parent_dir, &current_entry);

    match &x {
        Err(e) => {
            println!(
                "next_entry failed for path {} on current entry {} with error {}",
                &parent_dir.to_string_lossy(),
                &ce,
                e
            );
        }
        Ok(next) => {
            let n;
            match next {
                None => {
                    n = "".to_string();
                }
                Some(next) => {
                    n = next.to_string_lossy().to_string();
                }
            }
            println!(
                "next_entry after {} for path {} is {}",
                &parent_dir.to_string_lossy(),
                &ce,
                n
            );
        }
    }

    return x;
}

fn do_next_entry(
    parent_dir: &Path,
    current_entry: &Option<OsString>,
) -> io::Result<Option<OsString>> {
    let paths = fs::read_dir(parent_dir)?;
    let mut paths: Vec<_> = paths.map(|r| r.unwrap()).collect();
    paths.sort_by_key(|dir| dir.path());
    match current_entry {
        // special case for getting the first entry
        None => {
            if !paths.is_empty() {
                return Ok(Some(OsString::from(paths[0].path().file_name().unwrap())));
            }
            Ok(None)
        }
        // Typical case
        Some(ce) => {
            match paths
                .iter()
                .position(|x| x.path().file_name().unwrap() == ce)
            {
                None => Err(io::Error::new(
                    ErrorKind::NotFound,
                    format!(
                        "Failed to find previous dir {} in {}. Something is very wrong.",
                        ce.to_string_lossy(),
                        parent_dir.to_string_lossy()
                    ),
                )),
                Some(i) => {
                    if i < paths.len() - 1 {
                        return Ok(Some(OsString::from(
                            paths[i + 1].path().file_name().unwrap(),
                        )));
                    }
                    Ok(None)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::io::Write;
    use std::vec::Vec;

    fn build_path(root: &Path, rest: &[&str]) -> PathBuf {
        let mut pb = PathBuf::from(root);
        for elem in rest {
            pb.push(&elem);
        }
        return pb;
    }

    #[test]
    fn test_read_count_split() -> io::Result<()> {
        let v: Vec<u8> = Vec::from("hello");
        let mut br = io::BufReader::new(v.as_slice());
        let mut buf = Vec::new();

        read_count(&mut br, &mut buf, 2)?;
        assert_eq!(&buf[..], "he".as_bytes());

        read_count(&mut br, &mut buf, 2)?;
        assert_eq!(&buf[..], "hell".as_bytes());

        match read_count(&mut br, &mut buf, 2) {
            Ok(()) => {
                panic!("should have failed");
            }
            Err(e) => {
                assert_eq!(e.kind(), ErrorKind::UnexpectedEof);
            }
        }
        assert_eq!(&buf[..], "hello".as_bytes());

        Ok(())
    }

    #[test]
    fn test_read_count_exact() -> io::Result<()> {
        let v: Vec<u8> = Vec::from("hello");
        let mut br = io::BufReader::new(v.as_slice());
        let mut buf = Vec::new();

        read_count(&mut br, &mut buf, 1)?;
        assert_eq!(&buf[..], "h".as_bytes());

        read_count(&mut br, &mut buf, 1)?;
        assert_eq!(&buf[..], "he".as_bytes());

        read_count(&mut br, &mut buf, 1)?;
        assert_eq!(&buf[..], "hel".as_bytes());

        read_count(&mut br, &mut buf, 1)?;
        assert_eq!(&buf[..], "hell".as_bytes());

        read_count(&mut br, &mut buf, 1)?;
        assert_eq!(&buf[..], "hello".as_bytes());

        match read_count(&mut br, &mut buf, 1) {
            Ok(()) => {
                panic!("should have failed");
            }
            Err(e) => {
                assert_eq!(e.kind(), ErrorKind::UnexpectedEof);
            }
        }
        assert_eq!(&buf[..], "hello".as_bytes());

        Ok(())
    }

    #[test]
    fn test_read_count_via_file() -> io::Result<()> {
        let v: Vec<u8> = Vec::from("hello.");

        let tempdir = tempfile::tempdir()?;
        let outname = tempdir.path().join("test.out");

        let mut outfile = File::create(&outname)?;
        let mut infile = io::BufReader::new(File::open(&outname)?);

        let mut buf = Vec::new();

        let mut misses = 0;
        let mut found = 0;
        let mut total_len = 0;

        let expected_len = v.len();

        // write one byte at a time, trying at each iteration to get the next bit
        let mut x = 0;
        while buf.len() < expected_len {
            // write the next char, if there are any left to write
            if x < expected_len {
                println!("writing {}", v[x] as char);
                outfile.write(&[v[x]])?;
            }

            let pos = infile.seek(SeekFrom::Current(0))?;
            let l = buf.len();

            // try to get one
            match read_count(&mut infile, &mut buf, 2) {
                Ok(()) => {
                    found += 1;
                    total_len += 2;
                }
                Err(e) => {
                    if e.kind() != ErrorKind::UnexpectedEof {
                        return Err(e);
                    }
                    infile.seek(SeekFrom::Start(pos))?;
                    buf.truncate(l);
                    misses += 1;
                }
            }
            x += 1;
            println!("buf is now {}", String::from_utf8_lossy(&buf));
            println!("x is now {}", x);
        }

        assert_eq!(misses, 3);
        assert_eq!(found, 3);
        assert_eq!(misses, expected_len - found);
        assert_eq!(expected_len, total_len);
        assert_eq!(v, buf);

        return Ok(());
    }

    #[test]
    fn test_single_reader() -> io::Result<()> {
        let bytes =
            include_bytes!("/mnt/mirr2/subset/2019-07-23/DMRC_2019-07-23_23BDA5A300008C43.log");
        let mut v: Vec<u8> = Vec::new();
        v.extend_from_slice(bytes);

        let tempdir = tempfile::tempdir()?;

        let outname = tempdir.path().join("test.out");

        let mut f = File::create(&outname)?;

        let mut sr = SingleEntryReader::open(Path::new(&outname))?;

        let mut buf = Vec::new();
        let mut misses = 0;
        let mut found = 0;
        let mut total_len = 0;

        let expected_len = v.len() as u64;

        // write one byte at a time, trying at each iteration to get the next bit
        for it in &v {
            // write the next char
            f.write(&[*it])?;

            // try to get one
            match sr.next(&mut buf)? {
                Some(len) => {
                    found += 1;
                    total_len += len;
                }
                None => {
                    misses += 1;
                }
            }
        }

        assert_eq!(found, 58);
        assert_eq!(misses, 771596);
        assert_eq!(misses, expected_len - found);
        assert_eq!(expected_len, total_len);
        assert_eq!(v, buf);

        return Ok(());
    }

    #[test]
    fn test_multi_file_reader_empty() {
        let tmpdir = tempfile::tempdir().unwrap();
        let dir_name = tmpdir.path().join("empty_dir");
        fs::create_dir_all(&dir_name).unwrap();

        assert_eq!(determine_next_location(&dir_name, &None).unwrap(), None);
    }

    #[test]
    fn test_multi_file_reader_single_file() {
        let tmpdir = tempfile::tempdir().unwrap();

        let dir_name = build_path(&tmpdir.path(), &["123", "456"]);
        fs::create_dir_all(&dir_name).unwrap();

        {
            let file_name = build_path(&tmpdir.path(), &["123", "456", "file"]);
            let mut f = fs::File::create(file_name).unwrap();
            f.write_all(b"data").unwrap();
            f.sync_all().unwrap();
        }

        let expected_dir = OsString::from("123");
        let expected_name = OsString::from("456");
        assert_eq!(
            determine_next_location(tmpdir.path(), &None).unwrap(),
            Some(FileLocation {
                dir: expected_dir,
                name: expected_name
            })
        );
    }

    #[test]
    fn test_multi_file_reader_two_in_same_dir() {
        let tmpdir = tempfile::tempdir().unwrap();

        let dir_name = build_path(&tmpdir.path(), &["123"]);
        fs::create_dir_all(&dir_name).unwrap();
        {
            let mut f = fs::File::create(build_path(&tmpdir.path(), &["123", "456"])).unwrap();
            f.write_all(b"data").unwrap();
            f.sync_all().unwrap();
        }
        {
            let mut f = fs::File::create(build_path(&tmpdir.path(), &["123", "789"])).unwrap();
            f.write_all(b"data").unwrap();
            f.sync_all().unwrap();
        }

        let loc_123_456 = Some(FileLocation {
            dir: OsString::from("123"),
            name: OsString::from("456"),
        });

        let loc_123_789 = Some(FileLocation {
            dir: OsString::from("123"),
            name: OsString::from("789"),
        });

        assert_eq!(
            determine_next_location(&tmpdir.path(), &None).unwrap(),
            loc_123_456,
        );
        assert_eq!(
            determine_next_location(&tmpdir.path(), &loc_123_456).unwrap(),
            loc_123_789,
        );
        assert_eq!(
            determine_next_location(&tmpdir.path(), &loc_123_789).unwrap(),
            None,
        );
    }

    #[test]
    fn test_multi_file_reader_new_dir_and_file() {
        let tmpdir = tempfile::tempdir().unwrap();

        let dir_name = build_path(&tmpdir.path(), &["123"]);
        fs::create_dir_all(&dir_name).unwrap();
        {
            let mut f = fs::File::create(build_path(&tmpdir.path(), &["123", "456"])).unwrap();
            f.write_all(b"data").unwrap();
            f.sync_all().unwrap();
        }

        let loc_123_456 = Some(FileLocation {
            dir: OsString::from("123"),
            name: OsString::from("456"),
        });

        assert_eq!(
            determine_next_location(tmpdir.path(), &None).unwrap(),
            loc_123_456,
        );
        assert_eq!(
            determine_next_location(tmpdir.path(), &loc_123_456).unwrap(),
            None,
        );

        fs::create_dir_all(build_path(tmpdir.path(), &["456"])).unwrap();
        {
            let mut f = fs::File::create(build_path(&tmpdir.path(), &["123", "567"])).unwrap();
            f.write_all(b"data").unwrap();
            f.sync_all().unwrap();
        }
        {
            let mut f = fs::File::create(build_path(&tmpdir.path(), &["456", "789"])).unwrap();
            f.write_all(b"data").unwrap();
            f.sync_all().unwrap();
        }
        //

        let loc_123_567 = Some(FileLocation {
            dir: OsString::from("123"),
            name: OsString::from("567"),
        });

        assert_eq!(
            determine_next_location(&tmpdir.path(), &loc_123_456).unwrap(),
            loc_123_567,
        );
    }

    #[test]
    fn test_multi_file_reader_new_dir() {
        let tmpdir = tempfile::tempdir().unwrap();

        let dir_name = build_path(&tmpdir.path(), &["123"]);
        fs::create_dir_all(&dir_name).unwrap();
        {
            let mut f = fs::File::create(build_path(&tmpdir.path(), &["123", "456"])).unwrap();
            f.write_all(b"data").unwrap();
            f.sync_all().unwrap();
        }

        let loc_123_456 = Some(FileLocation {
            dir: OsString::from("123"),
            name: OsString::from("456"),
        });

        assert_eq!(
            determine_next_location(tmpdir.path(), &None).unwrap(),
            loc_123_456,
        );
        assert_eq!(
            determine_next_location(tmpdir.path(), &loc_123_456).unwrap(),
            None,
        );

        // now create a new file in a new dir
        fs::create_dir_all(build_path(tmpdir.path(), &["456"])).unwrap();
        {
            let mut f = fs::File::create(build_path(&tmpdir.path(), &["456", "789"])).unwrap();
            f.write_all(b"data").unwrap();
            f.sync_all().unwrap();
        }

        let loc_456_789 = Some(FileLocation {
            dir: OsString::from("456"),
            name: OsString::from("789"),
        });

        assert_eq!(
            determine_next_location(tmpdir.path(), &loc_123_456).unwrap(),
            loc_456_789,
        );
    }

    #[test]
    fn test_multi_file_reader_missing_file() {
        let tmpdir = tempfile::tempdir().unwrap();

        let dir_name = build_path(&tmpdir.path(), &["123"]);
        fs::create_dir_all(&dir_name).unwrap();
        {
            let mut f = fs::File::create(build_path(&tmpdir.path(), &["123", "456"])).unwrap();
            f.write_all(b"data").unwrap();
            f.sync_all().unwrap();
        }
        {
            let mut f = fs::File::create(build_path(&tmpdir.path(), &["123", "567"])).unwrap();
            f.write_all(b"data").unwrap();
            f.sync_all().unwrap();
        }

        let loc_123_456 = Some(FileLocation {
            dir: OsString::from("123"),
            name: OsString::from("456"),
        });

        assert_eq!(
            determine_next_location(tmpdir.path(), &None).unwrap(),
            loc_123_456,
        );

        fs::remove_file(build_path(tmpdir.path(), &["123", "456"])).unwrap();

        match determine_next_location(tmpdir.path(), &loc_123_456) {
            Ok(_loc) => panic!("should not have returned next location"),
            Err(e) => {
                assert_eq!(
                    format!(
                        "Failed to find previous dir 456 in {}. Something is very wrong.",
                        build_path(tmpdir.path(), &["123"]).to_string_lossy()
                    ),
                    format!("{}", e)
                );
            }
        }
    }
}
