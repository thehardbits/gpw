use clap::Parser;
use gpwgen::{args::Args, generate::gen_to_disk, gpwascii::GpwAscii};
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::PathBuf,
};

fn main() {
    let args = Args::parse();
    let mut rdr = BufReader::new(File::open(&args.src).unwrap());
    let data = GpwAscii::parse(&mut rdr).unwrap();

    // Create the path to the output file with H3 resolution added and
    // gpwh3 extension.
    let dst_path = {
        let mut dst = PathBuf::new();
        dst.push(args.out);
        dst.push(args.src.file_name().unwrap());
        dst.set_extension(format!("res{}.h3tess", args.res));
        dst
    };
    let mut dst = BufWriter::new(File::create(dst_path).unwrap());
    gen_to_disk(data, &mut dst);
}
