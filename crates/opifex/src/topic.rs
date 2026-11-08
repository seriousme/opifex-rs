#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicFilter {
    pub value: String,
}

impl TopicFilter {
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
        }
    }

    pub fn matches(&self, topic: &str) -> bool {
        matches_topic_filter(topic, &self.value)
    }
}

pub fn matches_topic_filter(topic: &str, filter: &str) -> bool {
    if topic.is_empty() && filter.is_empty() {
        return true;
    }

    if filter == "#" {
        return true;
    }

    if topic.starts_with('$') && !filter.starts_with('$') {
        return false;
    }

    if filter.starts_with('$') && !topic.starts_with('$') {
        return false;
    }

    let topic_levels: Vec<&str> = topic.split('/').collect();
    let filter_levels: Vec<&str> = filter.split('/').collect();

    let mut topic_index = 0usize;
    let mut filter_index = 0usize;

    while filter_index < filter_levels.len() && topic_index < topic_levels.len() {
        match filter_levels[filter_index] {
            "#" => return true,
            "+" => {
                topic_index += 1;
                filter_index += 1;
            }
            current_filter => {
                if current_filter != topic_levels[topic_index] {
                    return false;
                }
                topic_index += 1;
                filter_index += 1;
            }
        }
    }

    if filter_index == filter_levels.len() && topic_index == topic_levels.len() {
        return true;
    }

    if filter_index == filter_levels.len() - 1 && filter_levels[filter_index] == "#" {
        return true;
    }

    if filter_index < filter_levels.len() && filter_levels[filter_index] == "#" {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::{matches_topic_filter, TopicFilter};

    #[test]
    fn filters_match_expected_mqtt_patterns() {
        assert!(matches_topic_filter("home/lights", "home/#"));
        assert!(matches_topic_filter("home/lights/bedroom", "home/#"));
        assert!(matches_topic_filter("home/lights", "home/+"));
        assert!(!matches_topic_filter("home/lights", "home/+/bedroom"));
        assert!(matches_topic_filter("kitchen/temperature", "+/temperature"));
        assert!(!matches_topic_filter(
            "kitchen/temperature/extra",
            "+/temperature"
        ));
        assert!(!matches_topic_filter("home/lights", "garden/#"));
        assert!(matches_topic_filter("$SYS/broker/uptime", "$SYS/#"));
        assert!(!matches_topic_filter("home/lights", "$SYS/#"));
    }

    #[test]
    fn topic_filter_wrapper_matches() {
        let filter = TopicFilter::new("sensor/+");
        assert!(filter.matches("sensor/bedroom"));
        assert!(!filter.matches("sensor/bedroom/extra"));
    }
}
