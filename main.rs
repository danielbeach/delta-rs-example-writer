use csv;
use deltalake;
use serde_json::json;


#[tokio::main]
async fn main() {
    // Setup DELTA table.
    let table = deltalake::open_table("./trips").await.unwrap();
    let mut wrtr = deltalake::writer::BufferedJsonWriter::try_new(table).unwrap();

    // Read CSV data to write to Delta table
    let mut reader = csv::Reader::from_path("202105-divvy-tripdata.csv");
    for result in reader.expect("REASON").records() {
        let record = result.unwrap();
        let ride_id = &record[0];
        let rideable_type = &record[1];
        let started_at = &record[2];
        let ended_at = &record[3];
        let start_station_name = &record[4];
        let start_station_id = &record[5];
        let end_station_name = &record[6];
        let end_station_id = &record[7];
        let start_lat = &record[8];
        let start_lng = &record[9];
        let end_lat = &record[10];
        let end_lng = &record[11];
        let member_casual = &record[12];
        let v = json!({
                    "ride_id": &ride_id,
                    "rideable_type": &rideable_type,
                    "started_at": &started_at,
                    "ended_at": &ended_at,
                    "start_station_name": &start_station_name,
                    "start_station_id": &start_station_id,
                    "end_station_name": &end_station_name,
                    "end_station_id": &end_station_id,
                    "start_lat": &start_lat,
                    "start_lng": &start_lng,
                    "end_lat": &end_lat,
                    "end_lng": &end_lng,
                    "member_casual": &member_casual
                });
        wrtr.write(v, deltalake::writer::WriterPartition::NoPartitions).unwrap();
    }


    let cnt = wrtr.count(&deltalake::writer::WriterPartition::NoPartitions);
    println!("{:?}", cnt);
    wrtr.flush().await.unwrap();
}
