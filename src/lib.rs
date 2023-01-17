#![allow(unused_imports)]

use geo::{coord, line_string, Coordinate, Polygon};
use hextree::h3ron::{self, H3Cell, Index};
use num_traits::Zero;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufRead, BufReader, Lines},
};

// $ head -n6    gpw_v4_population_count_rev11_2020_30_sec_1.asc
// ncols         10800
// nrows         10800
// xllcorner     -180
// yllcorner     -4.2632564145606e-14
// cellsize      0.0083333333333333
// NODATA_value  -9999
#[derive(Debug, Clone, PartialEq, Default)]
struct GpwAsciiHeader {
    ncols: usize,
    nrows: usize,
    xllcorner: f32,
    yllcorner: f32,
    cellsize: f32,
    nodata_value: String,
}

#[derive(Debug)]
enum GpwError {
    Io(io::Error),
    /// Generic parsing error
    Parse(&'static str, Option<Box<dyn std::fmt::Debug>>),
}

impl From<io::Error> for GpwError {
    fn from(e: io::Error) -> Self {
        GpwError::Io(e)
    }
}

impl<E: std::fmt::Debug + 'static> From<(&'static str, E)> for GpwError {
    fn from((field, e): (&'static str, E)) -> Self {
        GpwError::Parse(field, Some(Box::new(e)))
    }
}

impl GpwAsciiHeader {
    fn parse<R: std::io::Read>(rdr: &mut BufReader<R>) -> Result<Self, GpwError> {
        let mut ncols: Option<usize> = None;
        let mut nrows: Option<usize> = None;
        let mut xllcorner: Option<f32> = None;
        let mut yllcorner: Option<f32> = None;
        let mut cellsize: Option<f32> = None;
        let mut nodata_value: Option<String> = None;

        for _ in 0..6 {
            let mut line = String::new();
            rdr.read_line(&mut line)?;
            let mut tokens = line.split_whitespace();
            if let Some(token) = tokens.next() {
                match token {
                    "ncols" => {
                        ncols = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("ncols", None))?
                                .parse::<usize>()
                                .map_err(|e| ("ncols", e))?,
                        );
                    }
                    "nrows" => {
                        nrows = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("nrows", None))?
                                .parse::<usize>()
                                .map_err(|e| ("ncols", e))?,
                        );
                    }
                    "xllcorner" => {
                        xllcorner = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("xllcorner", None))?
                                .parse::<f32>()
                                .map_err(|e| ("xllcorner", e))?,
                        );
                    }
                    "yllcorner" => {
                        yllcorner = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("yllcorner", None))?
                                .parse::<f32>()
                                .map_err(|e| ("yllcorner", e))?,
                        );
                    }
                    "cellsize" => {
                        cellsize = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("cellsize", None))?
                                .parse::<f32>()
                                .map_err(|e| ("cellsize", e))?,
                        );
                    }
                    "NODATA_value" => {
                        nodata_value = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("NODATA_value", None))?
                                .to_string(),
                        );
                    }
                    unexpected_token => {
                        Err(("unexpected header token", unexpected_token.to_string()))?
                    }
                }
            }
        }

        if let (
            Some(ncols),
            Some(nrows),
            Some(xllcorner),
            Some(yllcorner),
            Some(cellsize),
            Some(nodata_value),
        ) = (ncols, nrows, xllcorner, yllcorner, cellsize, nodata_value)
        {
            Ok(Self {
                ncols,
                nrows,
                xllcorner,
                yllcorner,
                cellsize,
                nodata_value,
            })
        } else {
            Err(GpwError::Parse("incomplete header", None))
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct GpwAscii {
    header: GpwAsciiHeader,
    data: Vec<Vec<Option<f32>>>,
    filename: Option<String>,
}

impl GpwAscii {
    fn parse<R: std::io::Read>(rdr: &mut BufReader<R>) -> Result<Self, GpwError> {
        let header = GpwAsciiHeader::parse(rdr)?;
        let mut data = Vec::with_capacity(header.nrows);
        let mut data_line = String::new();
        let mut row_idx = 0;
        while 0 != rdr.read_line(&mut data_line)? {
            let mut row = Vec::with_capacity(header.ncols);
            for (col_idx, cell) in data_line.split_whitespace().enumerate() {
                let sample = if cell == header.nodata_value {
                    None
                } else {
                    Some(cell.parse::<f32>().map_err(|e| {
                        (
                            "cell parse error",
                            format!("row {}, col {}, err {}", row_idx, col_idx, e),
                        )
                    })?)
                };
                row.push(sample);
            }
            debug_assert_eq!(row.len(), header.ncols);
            row_idx += 1;
            data.push(row);
            data_line.clear();
        }
        debug_assert_eq!(data.len(), header.nrows);
        Ok(Self {
            header,
            data,
            filename: None,
        })
    }
}

// pub fn parse_asc(name: String) -> io::Result<HashMap<H3Cell, f64>> {
//     let file = File::open(name).expect("file not found!");
//     let buf_reader = BufReader::new(file);

//     let mut ncols = 0;
//     let mut nrows = 0;
//     let mut xllcorner = 0.0;
//     let mut yllcorner = 0.0;
//     let mut pos: Coordinate = Zero::zero();
//     let mut cellsize = 0.0;
//     let mut nodata = "-1".to_string();
//     let mut header_done = false;
//     let mut col = 0;
//     // let mut row = 0;

//     //let mut hexmap = HexTreeMap::new();
//     let mut map = HashMap::new();

//     for line in buf_reader.lines() {
//         let line = line?;
//         let mut tokens = line.split_whitespace();
//         if header_done {
//             for valstr in tokens {
//                 if valstr != nodata && valstr != "0" {
//                     let val = valstr.parse::<f64>().unwrap();
//                     // compute the 4 corners of the cell
//                     // clockwise winding order, closed linestring, no interior ring
//                     let cell = Polygon::new(
//                         line_string![
//                             pos,
//                             coord! {x: pos.x + cellsize, y: pos.y},
//                             coord! {x: pos.x + cellsize, y: pos.y - cellsize},
//                             coord! {x: pos.x, y: pos.y-cellsize},
//                             pos
//                         ],
//                         vec![],
//                     );
//                     // tesselate at res 10 so we can handle the two coordinate systems drifting
//                     let hexes = h3ron::polygon_to_cells(&cell, 10);

//                     for hex in hexes.unwrap().iter() {
//                         map.insert(H3Cell::new(*hex), val);
//                     }
//                 }
//                 col += 1;
//                 let offset = coord! { x: cellsize, y: 0.0};
//                 pos = pos + offset;
//                 if col >= ncols {
//                     col = 0;
//                     // row += 1;
//                     pos = coord! { x: xllcorner, y: pos.y - cellsize};
//                 }
//             }
//         } else {
//             let key = tokens.next();
//             if key == Some("ncols") {
//                 ncols = tokens.next().unwrap().parse::<u64>().unwrap();
//             } else if key == Some("nrows") {
//                 nrows = tokens.next().unwrap().parse::<u64>().unwrap();
//             } else if key == Some("xllcorner") {
//                 xllcorner = tokens.next().unwrap().parse::<f64>().unwrap();
//             } else if key == Some("yllcorner") {
//                 yllcorner = tokens.next().unwrap().parse::<f64>().unwrap();
//             } else if key == Some("cellsize") {
//                 cellsize = tokens.next().unwrap().parse::<f64>().unwrap();
//             } else if key == Some("NODATA_value") {
//                 nodata = tokens.next().unwrap().to_string();
//                 header_done = true;
//                 pos = coord! { x: xllcorner,
//                 y: yllcorner + (cellsize * nrows as f64)};
//                 println!("start is {:?}", pos);
//             }
//         }
//     }

//     let mut output = HashMap::new();
//     // compact the hexes back up to res 8
//     // fold each key in the map, find the parent at res 8, then find all the res 10 children
//     // then for each of the children, look for their population densities (or 0 if not found) and
//     // average them
//     for hex in map.keys() {
//         let parent = hex.get_parent(8).unwrap();
//         if !output.contains_key(&parent) {
//             let children = parent.get_children(10).unwrap();
//             let mut population_sum = 0.0;
//             for child in children.iter() {
//                 if let Some(pop) = map.get(&child) {
//                     population_sum += pop
//                 }
//             }
//             let population = population_sum / children.count() as f64;
//             output.insert(parent, population);
//         }
//     }

//     Ok(output)
// }

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_parse_header() {
        let header = r#"ncols         10800
nrows         10800
xllcorner     -180
yllcorner     -4.2632564145606e-14
cellsize      0.0083333333333333
NODATA_value  -9999
"#;
        let mut rdr = BufReader::new(Cursor::new(header));
        GpwAsciiHeader::parse(&mut rdr).unwrap();
    }

    #[test]
    fn test_parse() {
        let file = r#"ncols         4
nrows         4
xllcorner     -180
yllcorner     -4.2632564145606e-14
cellsize      0.0083333333333333
NODATA_value  -9999
-9999 -9999 -9999 -9999
-9999 -9999 -9999 -9999
-9999 -9999 -9999 -9999
-9999 -9999 -9999 -9999
"#;
        let file = File::open("/Volumes/dev/he/gpw/local/gpw-v4-population-count-rev11_2020_30_sec_asc/gpw_v4_population_count_rev11_2020_30_sec_1.asc").unwrap();
        let mut rdr = BufReader::new(file);
        let dataset = GpwAscii::parse(&mut rdr).unwrap();
        println!("{:?}", dataset);
    }

    // [test]
    // fn test_parse() {
    //     println!("cwd {:?}", std::env::current_dir());
    //     let mut res1: HashMap<H3Cell, f64> = HashMap::new();
    //     let mut res2: HashMap<H3Cell, f64> = HashMap::new();
    //     let mut res3: HashMap<H3Cell, f64> = HashMap::new();
    //     let mut res4: HashMap<H3Cell, f64> = HashMap::new();
    //     let mut res5: HashMap<H3Cell, f64> = HashMap::new();
    //     let mut res6: HashMap<H3Cell, f64> = HashMap::new();
    //     let mut res7: HashMap<H3Cell, f64> = HashMap::new();
    //     let mut res8: HashMap<H3Cell, f64> = HashMap::new();
    //     rayon::scope(|s| {
    //         s.spawn(|_| {
    //             res1 =
    //                 parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_1.asc".to_string())
    //                     .unwrap()
    //         });
    //         s.spawn(|_| {
    //             res2 =
    //                 parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_2.asc".to_string())
    //                     .unwrap()
    //         });
    //         s.spawn(|_| {
    //             res3 =
    //                 parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_3.asc".to_string())
    //                     .unwrap()
    //         });
    //         s.spawn(|_| {
    //             res4 =
    //                 parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_4.asc".to_string())
    //                     .unwrap()
    //         });
    //         s.spawn(|_| {
    //             res5 =
    //                 parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_5.asc".to_string())
    //                     .unwrap()
    //         });
    //         s.spawn(|_| {
    //             res6 =
    //                 parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_6.asc".to_string())
    //                     .unwrap()
    //         });
    //         s.spawn(|_| {
    //             res7 =
    //                 parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_7.asc".to_string())
    //                     .unwrap()
    //         });
    //         s.spawn(|_| {
    //             res8 =
    //                 parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_8.asc".to_string())
    //                     .unwrap()
    //         });
    //     });
    //     let mut popmap = HexTreeMap::new();
    //     for (cell, pop) in res1
    //         .into_iter()
    //         .chain(res2)
    //         .chain(res3)
    //         .chain(res4)
    //         .chain(res5)
    //         .chain(res6)
    //         .chain(res7)
    //         .chain(res8)
    //     {
    //         popmap.insert(cell, pop);
    //     }

    //     let mut f = BufWriter::new(File::create("/tmp/foo.bar").unwrap());
    //     serialize_into(&mut f, &popmap);
    // }
}
