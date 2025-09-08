use std::fmt;

use chrono::{DateTime, Datelike, Duration, Timelike, Utc, Weekday};

// use crate::schedule::Schedule;

#[derive(Debug, Clone)]
pub enum TimeUnit {
    Minutes(u32),
    Hours,
    Days,
    Weeks,
    Months,
}

#[derive(Debug, Clone)]
pub struct ScheduleBuilder {
    pub interval: Option<u32>,
    pub unit: Option<TimeUnit>,
    pub weekday: Option<Weekday>,
    pub time: Option<String>,
    pub at_second: Option<u32>,
}

impl ScheduleBuilder {
    pub fn new() -> Self {
        ScheduleBuilder {
            interval: None,
            unit: None,
            weekday: None,
            time: None,
            at_second: None,
        }
    }

    fn parse_time(&self) -> Option<(u32, u32, u32)> {
        self.time.as_ref().and_then(|time_str| {
            if time_str.starts_with(':') {
                // Format like ":17" means second 17 of current minute
                time_str[1..].parse::<u32>().ok().map(|sec| (0, 0, sec))
            } else {
                // Format like "10:30" or "13:15:30"
                let parts: Vec<&str> = time_str.split(':').collect();
                match parts.len() {
                    2 => {
                        let hour = parts[0].parse::<u32>().ok()?;
                        let min = parts[1].parse::<u32>().ok()?;
                        Some((hour, min, 0))
                    }
                    3 => {
                        let hour = parts[0].parse::<u32>().ok()?;
                        let min = parts[1].parse::<u32>().ok()?;
                        let sec = parts[2].parse::<u32>().ok()?;
                        Some((hour, min, sec))
                    }
                    _ => None,
                }
            }
        })
    }

    fn calculate_next_execution(&self, from: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let from_local = from.with_timezone(&Utc);

        match &self.unit {
            Some(TimeUnit::Minutes(n)) => {
                let interval = self.interval.unwrap_or(1);
                let minutes = if *n == 1 && self.interval.is_none() {
                    1
                } else {
                    *n * interval
                };

                if let Some((_, _, sec)) = self.parse_time() {
                    // For ":17" format - every minute at second 17
                    let mut next = from_local
                        .with_second(sec)
                        .unwrap_or(from_local)
                        .with_nanosecond(0)
                        .unwrap_or(from_local);

                    if next <= from_local {
                        next = next + Duration::minutes(1);
                    }
                    Some(next.with_timezone(&Utc))
                } else {
                    Some(from + Duration::minutes(minutes as i64))
                }
            }
            Some(TimeUnit::Hours) => {
                let interval = self.interval.unwrap_or(1);
                let next = if let Some((_, min, sec)) = self.parse_time() {
                    let mut next = from_local
                        .with_minute(min)
                        .unwrap_or(from_local)
                        .with_second(sec)
                        .unwrap_or(from_local)
                        .with_nanosecond(0)
                        .unwrap_or(from_local);

                    if next <= from_local {
                        next = next + Duration::hours(interval as i64);
                    }
                    next
                } else {
                    from + Duration::hours(interval as i64)
                };
                Some(next.with_timezone(&Utc))
            }
            Some(TimeUnit::Days) => {
                let interval = self.interval.unwrap_or(1);
                let next = if let Some((hour, min, sec)) = self.parse_time() {
                    let mut next = from_local
                        .with_hour(hour)
                        .unwrap_or(from_local)
                        .with_minute(min)
                        .unwrap_or(from_local)
                        .with_second(sec)
                        .unwrap_or(from_local)
                        .with_nanosecond(0)
                        .unwrap_or(from_local);

                    if next <= from_local {
                        next = next + Duration::days(interval as i64);
                    }
                    next
                } else {
                    from + Duration::days(interval as i64)
                };
                Some(next.with_timezone(&Utc))
            }
            Some(TimeUnit::Weeks) => {
                let interval = self.interval.unwrap_or(1);

                if let Some(weekday) = &self.weekday {
                    let days_until_target = (weekday.num_days_from_monday() as i64
                        - from_local.weekday().num_days_from_monday() as i64
                        + 7)
                        % 7;

                    let mut next = from_local + Duration::days(days_until_target);

                    if let Some((hour, min, sec)) = self.parse_time() {
                        next = next
                            .with_hour(hour)
                            .unwrap_or(next)
                            .with_minute(min)
                            .unwrap_or(next)
                            .with_second(sec)
                            .unwrap_or(next)
                            .with_nanosecond(0)
                            .unwrap_or(next);
                    }

                    if next <= from_local {
                        next = next + Duration::weeks(interval as i64);
                    }

                    Some(next.with_timezone(&Utc))
                } else {
                    Some(from + Duration::weeks(interval as i64))
                }
            }
            Some(TimeUnit::Months) => {
                // Simplified month calculation
                let interval = self.interval.unwrap_or(1);
                Some(from + Duration::days(30 * interval as i64))
            }
            None => None,
        }
    }
}

pub struct IntervalBuilder {
    interval: u32,
}

pub struct TimeUnitBuilder {
    schedule: ScheduleBuilder,
}

pub struct WeekdayBuilder {
    schedule: ScheduleBuilder,
}

impl ScheduleBuilder {
    pub fn every(&self, interval: u32) -> IntervalBuilder {
        IntervalBuilder { interval }
    }

    pub fn each(&self) -> TimeUnitBuilder {
        TimeUnitBuilder {
            schedule: ScheduleBuilder::new(),
        }
    }
}

impl IntervalBuilder {
    pub fn minutes(self) -> TimeUnitBuilder {
        TimeUnitBuilder {
            schedule: ScheduleBuilder {
                interval: Some(self.interval),
                unit: Some(TimeUnit::Minutes(self.interval)),
                ..ScheduleBuilder::new()
            },
        }
    }
}

impl TimeUnitBuilder {
    pub fn hour(mut self) -> TimeUnitBuilder {
        self.schedule.unit = Some(TimeUnit::Hours);
        self
    }

    pub fn day(mut self) -> TimeUnitBuilder {
        self.schedule.unit = Some(TimeUnit::Days);
        self
    }

    pub fn week(mut self) -> TimeUnitBuilder {
        self.schedule.unit = Some(TimeUnit::Weeks);
        self
    }

    pub fn monday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Mon);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    pub fn tuesday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Tue);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    pub fn wednesday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Wed);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    pub fn thursday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Thu);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    pub fn friday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Fri);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    pub fn saturday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Sat);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    pub fn sunday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Sun);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    pub fn minute(mut self) -> TimeUnitBuilder {
        self.schedule.unit = Some(TimeUnit::Minutes(1));
        self
    }

    pub fn at(mut self, time: &str) -> TimeUnitBuilder {
        self.schedule.time = Some(time.to_string());
        self
    }

    pub fn build(self) -> ScheduleBuilder {
        self.schedule
    }
}

impl WeekdayBuilder {
    pub fn at(mut self, time: &str) -> WeekdayBuilder {
        self.schedule.time = Some(time.to_string());
        self
    }

    pub fn build(self) -> ScheduleBuilder {
        self.schedule
    }
}

// Main schedule function to start the builder
pub fn schedule() -> ScheduleBuilder {
    ScheduleBuilder::new()
}

impl fmt::Display for ScheduleBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();

        if let Some(interval) = self.interval {
            parts.push(format!("every {} ", interval));
        } else {
            parts.push("every ".to_string());
        }

        match &self.unit {
            Some(TimeUnit::Minutes(1)) => parts.push("minute".to_string()),
            Some(TimeUnit::Minutes(n)) => parts.push(format!("{} minutes", n)),
            Some(TimeUnit::Hours) => parts.push("hour".to_string()),
            Some(TimeUnit::Days) => parts.push("day".to_string()),
            Some(TimeUnit::Weeks) => parts.push("week".to_string()),
            Some(TimeUnit::Months) => parts.push("month".to_string()),
            None => {}
        }

        if let Some(weekday) = &self.weekday {
            parts.push(format!("{:?}", weekday).to_lowercase());
        }

        if let Some(time) = &self.time {
            parts.push(format!("at {}", time));
        }

        write!(f, "{}", parts.join(" "))
    }
}

impl<Tz: chrono::TimeZone> crate::schedule::Schedule<Tz> for ScheduleBuilder {
    fn next_tick(&self, tz: &Tz) -> Option<DateTime<Tz>> {
        let now = Utc::now();
        let next = self.calculate_next_execution(now);
        next.map(|dt| dt.with_timezone(tz))
    }
}

#[cfg(test)]
mod tests {
    use apalis_core::{
        error::BoxDynError,
        task::task_id::TaskId,
        worker::{
            builder::WorkerBuilder, context::WorkerContext, event::Event,
            ext::event_listener::EventListenerExt,
        },
    };
    use ulid::Ulid;

    use crate::{backend::CronStream, tick::Tick};

    use super::*;

    use std::time::Duration;
    #[test]
    fn test_schedule_examples() {
        // Every 10 minutes
        let s1 = schedule().every(10).minutes().build();
        println!("{}", s1);

        // Every hour
        let s2 = schedule().each().hour().build();
        println!("{}", s2);

        // Every day at 10:30
        let s3 = schedule().each().day().at("10:30").build();
        println!("{}", s3);

        // Every Monday
        let s4 = schedule().each().monday().build();
        println!("{}", s4);

        // Every Wednesday at 13:15
        let s5 = schedule().each().wednesday().at("13:15").build();
        println!("{}", s5);

        // Every minute at second 17
        let s7 = schedule().each().minute().at(":17").build();
        println!("{}", s7);
    }

    #[tokio::test]
    async fn basic_worker() {
        let schedule = schedule().each().minute().at(":06").build();
        let backend = CronStream::new(schedule);

        async fn send_reminder(job: Tick, id: TaskId<Ulid>) -> Result<(), BoxDynError> {
            println!("Running cronjob for timestamp: {:?} with id {}", job, id);
            tokio::time::sleep(Duration::from_secs(1)).await;
            Err("All failing".into())
        }

        let worker = WorkerBuilder::new("rango-tango")
            .backend(backend)
            .on_event(move |ctx, ev| {
                println!("{:?}", ev);
                let ctx = ctx.clone();
                if matches!(ev, Event::Error(_)) {
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        ctx.stop().unwrap();
                    });
                }
            })
            .build(send_reminder);
        worker.run().await.unwrap();
    }
}
