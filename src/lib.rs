use geo::{coord, line_string, Polygon};
use hextree::h3ron;
use rayon::prelude::*;
use std::io::{self, BufRead, BufReader, Write};

// $ head -n6    gpw_v4_population_count_rev11_2020_30_sec_1.asc
// ncols         10800
// nrows         10800
// xllcorner     -180
// yllcorner     -4.2632564145606e-14
// cellsize      0.0083333333333333
// NODATA_value  -9999
#[derive(Debug, Clone, PartialEq, Default)]
pub struct GpwAsciiHeader {
    ncols: usize,
    nrows: usize,
    xllcorner: f64,
    yllcorner: f64,
    cellsize: f64,
    nodata_value: String,
}

#[derive(Debug)]
pub enum GpwError {
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
    pub fn parse<R: std::io::Read>(rdr: &mut BufReader<R>) -> Result<Self, GpwError> {
        let mut ncols: Option<usize> = None;
        let mut nrows: Option<usize> = None;
        let mut xllcorner: Option<f64> = None;
        let mut yllcorner: Option<f64> = None;
        let mut cellsize: Option<f64> = None;
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
                                .parse::<f64>()
                                .map_err(|e| ("xllcorner", e))?,
                        );
                    }
                    "yllcorner" => {
                        yllcorner = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("yllcorner", None))?
                                .parse::<f64>()
                                .map_err(|e| ("yllcorner", e))?,
                        );
                    }
                    "cellsize" => {
                        cellsize = Some(
                            tokens
                                .next()
                                .ok_or(GpwError::Parse("cellsize", None))?
                                .parse::<f64>()
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
pub struct GpwAscii {
    header: GpwAsciiHeader,
    data: Vec<Vec<Option<f32>>>,
    filename: Option<String>,
}

impl GpwAscii {
    pub fn parse<R: std::io::Read>(rdr: &mut BufReader<R>) -> Result<Self, GpwError> {
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
            assert_eq!(row.len(), header.ncols);
            row_idx += 1;
            data.push(row);
            data_line.clear();
        }
        assert_eq!(data.len(), header.nrows);
        Ok(Self {
            header,
            data,
            filename: None,
        })
    }
}

fn tessalate_grid(header: &GpwAsciiHeader, row: usize, col: usize) -> Vec<u64> {
    let grid_bottom_degs = header.yllcorner + header.cellsize * (header.nrows - row + 1) as f64;
    let grid_top_degs = grid_bottom_degs + header.cellsize;
    let grid_left_degs = header.xllcorner + header.cellsize * col as f64;
    let grid_right_degs = grid_left_degs + header.cellsize;

    let grid_cell_poly = Polygon::new(
        line_string![
            // lower-left
            coord! {x: grid_left_degs, y: grid_bottom_degs},
            // lower-right
            coord! {x: grid_right_degs, y: grid_bottom_degs},
            // upper-right
            coord! {x: grid_right_degs, y: grid_top_degs},
            // upper-left
            coord! {x: grid_left_degs, y: grid_top_degs},
            // lower-left
            coord! {x: grid_left_degs, y: grid_bottom_degs}
        ],
        vec![],
    );
    // Tesselate at res 10 so we can handle the two coordinate systems
    // drifting.
    let hexes = h3ron::polygon_to_cells(&grid_cell_poly, 10).unwrap();
    hexes.iter().map(|hex| *hex).collect()
}

pub fn gen_to_disk(src: GpwAscii, dst: &mut impl Write) {
    let (tx, rx) = std::sync::mpsc::channel::<(Vec<u64>, f32)>();

    let handle = std::thread::spawn(move || {
        let header = &src.header;
        let data = &src.data;
        data.into_par_iter()
            .enumerate()
            .for_each_with(tx, |tx, (row_idx, row)| {
                row.par_iter()
                    .enumerate()
                    .for_each_with(tx.clone(), |tx, (col_idx, sample)| {
                        if let Some(val) = sample {
                            let h3_indicies = tessalate_grid(header, row_idx, col_idx);
                            tx.send((h3_indicies, *val)).unwrap();
                        }
                    })
            })
    });

    while let Ok((h3_indicies, val)) = rx.recv() {
        let scaled_val = val / h3_indicies.len() as f32;
        let scaled_val_bytes = scaled_val.to_le_bytes();
        for h3_index in h3_indicies {
            dst.write_all(&h3_index.to_le_bytes()).unwrap();
            dst.write_all(&scaled_val_bytes).unwrap();
        }
    }
    handle.join().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs::File,
        io::{BufWriter, Cursor},
    };

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
-9999 -9999 0.123 -9999
"#;
        let mut rdr = BufReader::new(Cursor::new(file));
        GpwAscii::parse(&mut rdr).unwrap();
    }

    #[test]
    fn test_gen_to_disk() {
        let file = r#"ncols         4
nrows         4
xllcorner     -180
yllcorner     -4.2632564145606e-14
cellsize      0.0083333333333333
NODATA_value  -9999
-9999 -9999 -9999 -9999
-9999 -9999 -9999 -9999
-9999 -9999 -9999 -9999
-9999 -9999 0.123 -9999
"#;
        let mut rdr = BufReader::new(Cursor::new(file));
        let data = GpwAscii::parse(&mut rdr).unwrap();
        let mut dst = BufWriter::new(File::create("/Users/jay/he/gpw/out.indicies").unwrap());
        gen_to_disk(data, &mut dst);
    }
}
