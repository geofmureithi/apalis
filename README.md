# Actix-jobs
Efficient and reliable background processing for Rust using Actix and Redis

## Getting Started

To get started, just add to Cargo.toml 

```toml
[dependencies]
actix-jobs = { version = "0.1" }
```

### Prerequisites

A running redis server is required.
You can quickly use docker:
````bash
docker run --name some-redis -d redis
````

## Usage

````rust
let actor = QueueActor::new("redis://127.0.0.1/", Queue::new("fuseapp")).await;
let addr = Supervisor::start(move |_| actor);
Supervisor::start(move |_| Consumer::new(addr));
let producer = Producer::new(p_addr);
let task = Task {
    id: "1".to_string(),
    job: "SEND_SMS".to_string(),
    data: ......
};
producer.push_job(task).await;
````

## Built With

* [Actix](https://actix.rs) - Actor framework for Rust
* [Redis-Rs](https://github.com/mitsuhiko/redis-rs) - Redis library for rust 

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/geofmureithi/actix-jobs/tags). 

## Authors

* **Njuguna Mureithi** - *Initial work* - [Njuguna Mureithi](https://github.com/geofmureithi)

See also the list of [contributors](https://github.com/geofmureithi/actix-jobs/contributors) who participated in this project.

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Inspiration: This project is heavilly inspired by [Curlyq](https://github.com/mcmathja/curlyq) which is written in GoLang
