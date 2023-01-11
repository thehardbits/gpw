use std::fs;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;

use bincode::serialize_into;

use std::collections::HashMap;

use rayon::prelude::*;

use geo::Polygon;
use hexset::h3ron::H3Cell;
use hexset::h3ron::Index;
use hexset::HexMap;

use geo::coord;
use geo::line_string;
use geo::Coordinate;
use num_traits::Zero;

fn parse_asc(name: String) -> io::Result<HashMap<H3Cell, f64>> {
    let file = File::open(name).expect("file not found!");
    let buf_reader = BufReader::new(file);

    let mut ncols = 0;
    let mut nrows = 0;
    let mut xllcorner = 0.0;
    let mut yllcorner = 0.0;
    let mut pos: Coordinate = Zero::zero();
    let mut cellsize = 0.0;
    let mut nodata = "-1".to_string();
    let mut header_done = false;
    let mut col = 0;
    let mut row = 0;

    //let mut hexmap = HexMap::new();
    let mut map = HashMap::new();

    for line in buf_reader.lines() {
        let line = line?;
        let mut tokens = line.split_whitespace();
        if header_done == true {
            for valstr in tokens {
                if valstr != nodata && valstr != "0" {
                    let val = valstr.parse::<f64>().unwrap();
                    // compute the 4 corners of the cell
                    // clockwise winding order, closed linestring, no interior ring
                    let cell = Polygon::new(
                        line_string![
                            pos,
                            coord! {x: pos.x + cellsize, y: pos.y},
                            coord! {x: pos.x + cellsize, y: pos.y - cellsize},
                            coord! {x: pos.x, y: pos.y-cellsize},
                            pos
                        ],
                        vec![],
                    );
                    // tesselate at res 10 so we can handle the two coordinate systems drifting
                    let hexes = h3ron::polygon_to_cells(&cell, 10);

                    for hex in hexes.unwrap().iter() {
                        map.insert(H3Cell::new(*hex), val);
                    }
                }
                col += 1;
                let offset = coord! { x: cellsize, y: 0.0};
                pos = pos + offset;
                if col >= ncols {
                    col = 0;
                    row += 1;
                    pos = coord! { x: xllcorner, y: pos.y - cellsize};
                }
            }
        } else {
            let key = tokens.next();
            if key == Some("ncols") {
                ncols = tokens.next().unwrap().parse::<u64>().unwrap();
            } else if key == Some("nrows") {
                nrows = tokens.next().unwrap().parse::<u64>().unwrap();
            } else if key == Some("xllcorner") {
                xllcorner = tokens.next().unwrap().parse::<f64>().unwrap();
            } else if key == Some("yllcorner") {
                yllcorner = tokens.next().unwrap().parse::<f64>().unwrap();
            } else if key == Some("cellsize") {
                cellsize = tokens.next().unwrap().parse::<f64>().unwrap();
            } else if key == Some("NODATA_value") {
                nodata = tokens.next().unwrap().to_string();
                header_done = true;
                pos = coord! { x: xllcorner,
                y: yllcorner + (cellsize * nrows as f64)};
                println!("start is {:?}", pos);
            }
        }
    }

    let mut output = HashMap::new();
    // compact the hexes back up to res 8
    // fold each key in the map, find the parent at res 8, then find all the res 10 children
    // then for each of the children, look for their population densities (or 0 if not found) and
    // average them
    for hex in map.keys() {
        let parent = hex.get_parent(8).unwrap();
        if output.contains_key(&parent) == false {
            let children = parent.get_children(10).unwrap();
            let mut population_sum = 0.0;
            for child in children.iter() {
                match map.get(&child) {
                    Some(pop) => population_sum += pop,
                    None => (),
                }
            }
            let population = population_sum / children.count() as f64;
            output.insert(parent, population);
        }
    }

    return Ok(output);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_parse() {
        println!("cwd {:?}", std::env::current_dir());
        let mut res1: HashMap<H3Cell, f64> = HashMap::new();
        let mut res2: HashMap<H3Cell, f64> = HashMap::new();
        let mut res3: HashMap<H3Cell, f64> = HashMap::new();
        let mut res4: HashMap<H3Cell, f64> = HashMap::new();
        let mut res5: HashMap<H3Cell, f64> = HashMap::new();
        let mut res6: HashMap<H3Cell, f64> = HashMap::new();
        let mut res7: HashMap<H3Cell, f64> = HashMap::new();
        let mut res8: HashMap<H3Cell, f64> = HashMap::new();
        rayon::scope(|s| {
            s.spawn(|_| {
                res1 =
                    parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_1.asc".to_string())
                        .unwrap()
            });
            s.spawn(|_| {
                res2 =
                    parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_2.asc".to_string())
                        .unwrap()
            });
            s.spawn(|_| {
                res3 =
                    parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_3.asc".to_string())
                        .unwrap()
            });
            s.spawn(|_| {
                res4 =
                    parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_4.asc".to_string())
                        .unwrap()
            });
            s.spawn(|_| {
                res5 =
                    parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_5.asc".to_string())
                        .unwrap()
            });
            s.spawn(|_| {
                res6 =
                    parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_6.asc".to_string())
                        .unwrap()
            });
            s.spawn(|_| {
                res7 =
                    parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_7.asc".to_string())
                        .unwrap()
            });
            s.spawn(|_| {
                res8 =
                    parse_asc("data/gpw_v4_population_density_rev11_2020_30_sec_8.asc".to_string())
                        .unwrap()
            });
        });
        let mut popmap = HexMap::new();
        for (cell, pop) in res1
            .into_iter()
            .chain(res2)
            .chain(res3)
            .chain(res4)
            .chain(res5)
            .chain(res6)
            .chain(res7)
            .chain(res8)
        {
            popmap.insert(cell, pop);
        }

        let mut f = BufWriter::new(File::create("/tmp/foo.bar").unwrap());
        serialize_into(&mut f, &popmap);
    }
}
