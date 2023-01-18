#![deny(clippy::unwrap_used)]

mod options;
use anyhow::Result;
use byteorder::{LittleEndian as LE, ReadBytesExt};
use clap::Parser;
use hextree::{h3ron::H3Cell, HexTreeMap};
use hyper::{
    body::Body,
    service::{make_service_fn, service_fn},
    Error, Response, Server, StatusCode,
};
use std::{
    convert::TryFrom,
    fs::File,
    io::{BufReader, ErrorKind, Read},
    sync::Arc,
};

#[tokio::main]
async fn main() -> Result<()> {
    let args = options::Cli::parse();
    let f = BufReader::new(File::open(args.path)?);
    let map: HexTreeMap<f32> = deserialize_hexmap(f)?;
    let map = Arc::new(map);

    let make_service = make_service_fn(move |_| {
        let map = map.clone();
        async move {
            Ok::<_, Error>(service_fn(move |req| {
                let path = req.uri().path();
                let population = u64::from_str_radix(&path[1..], 16)
                    .ok()
                    .and_then(|index| H3Cell::try_from(index).ok())
                    .and_then(|cell| map.get(cell).cloned());
                async move {
                    match population {
                        Some(pop) => {
                            Ok::<_, Error>(Response::new(Body::from(format!("{:?}", pop))))
                        }
                        None => {
                            let mut not_found = Response::default();
                            *not_found.status_mut() = StatusCode::NOT_FOUND;
                            Ok(not_found)
                        }
                    }
                }
            }))
        }
    });

    let addr = ([127, 0, 0, 1], 3000).into();
    let server = Server::bind(&addr).serve(make_service);

    println!("Listening on http://{}", addr);

    server.await?;
    Ok(())
}

fn deserialize_hexmap(mut rdr: impl Read) -> Result<HexTreeMap<f32>> {
    let mut map = HexTreeMap::new();
    loop {
        match (rdr.read_u64::<LE>(), rdr.read_f32::<LE>()) {
            (Ok(h3_index), Ok(val)) => {
                let cell = H3Cell::try_from(h3_index)?;
                map.insert(cell, val)
            }
            (Err(e), _) if e.kind() == ErrorKind::UnexpectedEof => break,
            (err @ Err(_), _) => {
                err?;
            }
            (_, err @ Err(_)) => {
                err?;
            }
        };
    }
    Ok(map)
}
