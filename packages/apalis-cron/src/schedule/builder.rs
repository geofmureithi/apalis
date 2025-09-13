use std::fmt;

use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc, Weekday};

/// Time units for scheduling
#[derive(Debug, Clone)]
pub enum TimeUnit {
    /// Minutes time unit
    Minutes(u32),
    /// Hours time unit
    Hours,
    /// Days time unit
    Days,
    /// Weeks time unit
    Weeks,
    /// Months time unit
    Months,
}

/// Builder for creating schedules via a fluent API
#[derive(Debug, Clone)]
pub struct ScheduleBuilder {
    interval: Option<u32>,
    unit: Option<TimeUnit>,
    weekday: Option<Weekday>,
    time: Option<String>,
}

impl ScheduleBuilder {
    /// Creates a new `ScheduleBuilder`.
    pub fn new() -> Self {
        ScheduleBuilder {
            interval: None,
            unit: None,
            weekday: None,
            time: None,
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

/// Builder for creating intervals
#[derive(Debug, Clone)]
pub struct IntervalBuilder {
    interval: u32,
}

/// Builder for creating time units
#[derive(Debug, Clone)]
pub struct TimeUnitBuilder {
    schedule: ScheduleBuilder,
}

/// Builder for creating weekdays
#[derive(Debug, Clone)]
pub struct WeekdayBuilder {
    schedule: ScheduleBuilder,
}

impl ScheduleBuilder {
    /// Creates a new `IntervalBuilder` for specifying the interval.
    pub fn every(&self, interval: u32) -> IntervalBuilder {
        IntervalBuilder { interval }
    }

    /// Creates a new `TimeUnitBuilder` for specifying the time unit.
    pub fn each(&self) -> TimeUnitBuilder {
        TimeUnitBuilder {
            schedule: ScheduleBuilder::new(),
        }
    }
}

impl IntervalBuilder {
    /// Creates a new `TimeUnitBuilder` for specifying the time unit.
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
    /// Sets the time unit to hours.
    pub fn hour(mut self) -> TimeUnitBuilder {
        self.schedule.unit = Some(TimeUnit::Hours);
        self
    }

    /// Sets the time unit to days.
    pub fn day(mut self) -> TimeUnitBuilder {
        self.schedule.unit = Some(TimeUnit::Days);
        self
    }
    /// Sets the time unit to weeks.
    pub fn week(mut self) -> TimeUnitBuilder {
        self.schedule.unit = Some(TimeUnit::Weeks);
        self
    }
    /// Sets the day to Monday for the weekday schedule.
    pub fn monday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Mon);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    /// Sets the day to Tuesday for the weekday schedule.
    pub fn tuesday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Tue);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    /// Sets the day to Wednesday for the weekday schedule.
    pub fn wednesday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Wed);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    /// Sets the day to Thursday for the weekday schedule.
    pub fn thursday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Thu);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    /// Sets the day to Friday for the weekday schedule.
    pub fn friday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Fri);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    /// Sets the day to Saturday for the weekday schedule.
    pub fn saturday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Sat);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    /// Sets the day to Sunday for the weekday schedule.
    pub fn sunday(mut self) -> WeekdayBuilder {
        self.schedule.weekday = Some(Weekday::Sun);
        WeekdayBuilder {
            schedule: self.schedule,
        }
    }

    /// Sets the time unit to minutes.
    pub fn minute(mut self) -> TimeUnitBuilder {
        self.schedule.unit = Some(TimeUnit::Minutes(1));
        self
    }

    /// Sets the time
    pub fn at(mut self, time: &str) -> TimeUnitBuilder {
        self.schedule.time = Some(time.to_string());
        self
    }

    /// Builds the schedule iterator.
    pub fn build<Tz: TimeZone>(self) -> ScheduleIterator<Tz> {
        ScheduleIterator::new(self.schedule)
    }
}

impl WeekdayBuilder {
    /// Sets the time for the weekday schedule.
    pub fn at(mut self, time: &str) -> WeekdayBuilder {
        self.schedule.time = Some(time.to_string());
        self
    }

    /// Builds the weekday schedule.
    pub fn build<Tz: TimeZone>(self) -> ScheduleIterator<Tz> {
        ScheduleIterator::new(self.schedule)
    }
}

/// Iterator over schedule ticks
#[derive(Debug, Clone)]
pub struct ScheduleIterator<Tz = Utc>
where
    Tz: chrono::TimeZone,
{
    schedule: ScheduleBuilder,
    current: Option<DateTime<Tz>>,
}

impl<Tz: chrono::TimeZone> ScheduleIterator<Tz> {
    /// Create a new schedule iterator
    pub fn new(schedule: ScheduleBuilder) -> Self {
        Self {
            schedule,
            current: None,
        }
    }
}

impl<Tz> fmt::Display for ScheduleIterator<Tz>
where
    Tz: TimeZone,
    Tz::Offset: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScheduleIterator(schedule = {:?}, ", self.schedule)?;
        match &self.current {
            Some(dt) => write!(f, "next = {})", dt),
            None => write!(f, "end)"),
        }
    }
}

/// Builder for creating schedules via a fluent API
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

impl<Tz: chrono::TimeZone> crate::schedule::Schedule<Tz> for ScheduleIterator<Tz> {
    fn next_tick(&mut self, tz: &Tz) -> Option<DateTime<Tz>> {
        let current = self
            .current
            .take()
            .unwrap_or(Utc::now().with_timezone(&tz))
            .with_timezone(&Utc);
        let next = self
            .schedule
            .calculate_next_execution(current)
            .map(|dt| dt.with_timezone(tz));
        self.current = next.clone();
        next
    }
}

#[cfg(test)]
mod tests {
    use apalis_core::{
        error::BoxDynError,
        task::task_id::TaskId,
        worker::{builder::WorkerBuilder, event::Event, ext::event_listener::EventListenerExt},
    };
    use ulid::Ulid;

    use crate::{backend::CronStream, tick::Tick};

    use super::*;

    use std::time::Duration;
    #[test]
    fn test_schedule_examples() {
        // Every 10 minutes
        let s1 = schedule().every(10).minutes().build::<Utc>();
        println!("{}", s1);

        // Every hour
        let s2 = schedule().each().hour().build::<Utc>();
        println!("{}", s2);

        // Every day at 10:30
        let s3 = schedule().each().day().at("10:30").build::<Utc>();
        println!("{}", s3);

        // Every Monday
        let s4 = schedule().each().monday().build::<Utc>();
        println!("{}", s4);

        // Every Wednesday at 13:15
        let s5 = schedule().each().wednesday().at("13:15").build::<Utc>();
        println!("{}", s5);

        // Every minute at second 17
        let s7 = schedule().each().minute().at(":17").build::<Utc>();
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
