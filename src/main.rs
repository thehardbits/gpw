use clap::Parser;
use hextree::{
    h3ron::{H3Cell, Index},
    HexTreeMap,
};
use hyper::{
    body::Body,
    service::{make_service_fn, service_fn},
    Error, Response, Server, StatusCode,
};
use std::{fs::File, io::BufReader, sync::Arc};

/// Search for a pattern in a file and display the lines that contain it.
#[derive(Parser)]
struct Cli {
    /// The path to the file to read
    #[clap(parse(from_os_str))]
    path: std::path::PathBuf,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let f = BufReader::new(File::open(args.path).unwrap());
    let map: HexTreeMap<f64> = bincode::deserialize_from(f).unwrap();
    let map = Arc::new(map);

    let make_service = make_service_fn(move |_| {
        let map = map.clone();
        async move {
            // This is the `Service` that will handle the connection.
            // `service_fn` is a helper to convert a function that
            // returns a Response into a `Service`.
            Ok::<_, Error>(service_fn(move |req| {
                let path = req.uri().path();
                let h3idx = u64::from_str_radix(&path[1..], 16).unwrap();
                let result = map.get(H3Cell::new(h3idx)).cloned();
                async move {
                    match result {
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

    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

/*
#![allow(unused)]
use std::fs::File;
use std::io::BufReader;
use clap::Parser;

use bincode::deserialize_from;

use hexset::HexTreeMap;
use h3ron::H3Cell;
use h3ron::Index;

/// Search for a pattern in a file and display the lines that contain it.
#[derive(Parser)]
struct Cli {
    /// The pattern to look for
    pattern: u64,
    /// The path to the file to read
    #[clap(parse(from_os_str))]
    path: std::path::PathBuf,
}

fn main() {

    let args = Cli::parse();


    let t1 = std::time::Instant::now();
    let mut f = BufReader::new(File::open(args.path).unwrap());
    let t2 = std::time::Instant::now();

    let map: HexTreeMap<f64> = bincode::deserialize_from(f).unwrap();
    let t3 = std::time::Instant::now();

    let result = map.get(&H3Cell::new(args.pattern));
    let t4 = std::time::Instant::now();
    println!("result {:?}", result);
    println!("load time {:?}, deserialize time {:?}, query time {:?}", t2 - t1, t3 - t2, t4 - t3);


}
*/
