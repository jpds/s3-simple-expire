//
// s3-simple-expire - takes an S3 bucket and deletes objects older than a number
//                    of days. Intended for use with an S3 provider with no
//                    storage lifecycle policies.
//
// Copyright (C) 2023 Jonathan Davies
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//

#![allow(clippy::result_large_err)]

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{config::Region, Client, Error};
use aws_smithy_types_convert::date_time::DateTimeExt;
use chrono::{Days, Utc};
use clap::Parser;

#[derive(Debug, Parser)]
struct Opt {
    /// The name of the bucket.
    #[structopt(short, long)]
    bucket: String,

    /// Number of days to wait for
    #[structopt(short, long)]
    days: u64,

    /// Whether to look for, but not delete objects
    #[structopt(long)]
    dry_run: bool,

    /// The AWS endpoint.
    #[structopt(short, long, env = "AWS_ENDPOINT")]
    endpoint: Option<String>,

    /// The AWS Region.
    #[structopt(short, long, env = "AWS_DEFAULT_REGION")]
    region: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let Opt {
        bucket,
        days,
        dry_run,
        endpoint,
        region,
    } = Opt::parse();

    let now = Utc::now();
    let days_expiry = now
        .checked_sub_days(Days::new(days))
        .expect("Invalid number of days to subtract");

    let region_provider = RegionProviderChain::first_try(region.map(Region::new))
        .or_default_provider()
        .or_else(Region::new("us-east-1"));

    let shared_config = aws_config::from_env()
        .endpoint_url(endpoint.expect("No endpoint specified"))
        .region(region_provider)
        .load()
        .await;
    let client = Client::new(&shared_config);

    let resp = client.list_objects_v2().bucket(&bucket).send().await?;

    for object in resp.contents().unwrap_or_default() {
        let object_timestamp: chrono::DateTime<Utc> = object
            .last_modified
            .expect("Object does not have a last modified metadata entry")
            .to_chrono_utc()
            .expect("Error converting last modified datetime");

        if object_timestamp < days_expiry {
            println!(
                "{} is older than {} days, deleting...",
                object.key().unwrap_or_default(),
                days
            );

            if !dry_run {
                client
                    .delete_object()
                    .bucket(&bucket)
                    .key(object.key().unwrap_or_default())
                    .send()
                    .await?;
            }

            println!("{} deleted", object.key().unwrap_or_default())
        }
    }

    Ok(())
}
