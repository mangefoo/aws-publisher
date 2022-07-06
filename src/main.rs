use std::collections::HashMap;
use std::ops::Add;
use std::thread;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_costexplorer::{Client, Error};
use aws_sdk_costexplorer::model::{DateInterval};
use aws_sdk_costexplorer::model::Granularity::Monthly;
use chrono::{Datelike, DateTime, Duration, Local};
use chrono;
use serde_json::json;
use reqwest;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let update_frequency: i64 = 43200;
    let publish_frequency: u64 = 60;

    let (mut cost, mut next_update) = get_and_publish_cost(update_frequency).await;

    loop {
        thread::sleep(std::time::Duration::from_secs(publish_frequency));
        if next_update <= Local::now() {
            println!("Getting and publishing cost");
            (cost, next_update) = get_and_publish_cost(update_frequency).await;
        } else {
            println!("Publishing old cost");
            publish_cost(cost).await;
        }
    }
}

async fn get_and_publish_cost(update_frequency: i64) -> (f32, DateTime<Local>) {
    let cost = get_cost().await;
    publish_cost(cost).await;

    (cost, Local::now().add(Duration::seconds(update_frequency)))
}

async fn get_cost() -> f32 {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::from_env().region(region_provider).load().await;
    let client = Client::new(&config);

    let resp = client.get_cost_and_usage()
        .granularity(Monthly)
        .time_period(DateInterval::builder()
            .start(get_start_date())
            .end(get_end_date())
            .build())
        .metrics("UnblendedCost")
        .send().await.unwrap();

    println!("Start: {}, end: {}", get_start_date(), get_end_date());

    let results_by_time = resp.results_by_time().unwrap().get(0).unwrap();
//    println!("Cost: {:?}", results_by_time.get(0).unwrap().total.as_ref().unwrap().get("UnblendedCost"));

    results_by_time.total.as_ref().unwrap().get("UnblendedCost").unwrap().amount.as_ref().unwrap().parse::<f32>().unwrap()
}

fn get_start_date() -> String {
    let now = chrono::offset::Local::now().date();
    format!("{}-{:02}-01", now.year(), now.month())
}

fn get_end_date() -> String {
    let tomorrow = chrono::offset::Local::now().date().add(Duration::days(1));
    format!("{}-{:02}-{:02}", tomorrow.year(), tomorrow.month(), tomorrow.day())
}

async fn publish_cost(cost: f32) {
    let request_url = format!("http://sensor-relay.int.mindphaser.se/publish");

    let mut request_body = HashMap::<String, String>::new();

    request_body.insert("cost".to_string(), format!("{:.2}", cost));

    let body = json!({
            "reporter": "aws-publisher",
            "sensors": request_body,
            "topic": "sensors"
        });

    let post_response = reqwest::Client::new()
        .post(request_url)
        .json(&body)
        .send().await;

    if post_response.is_err() {
        println!("Failed to send update to server: {}", post_response.unwrap_err())
    } else {
        println!("Published rates OK {:?}", request_body);
    }
}