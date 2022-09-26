use rand::random;
use rand::seq::SliceRandom;
use std::collections::*;

const DESIRED_KEY_COUNT: usize = 10_000;

pub trait RandomKeys {
    fn random_keys_potentially_ordered(&self) -> Vec<String>;

    /// Retrieve shuffled keys to be used for testing
    /// Fetching the string from the sorted vector first,
    /// would make it possibly likely, that the memory page for the element is still loaded
    /// and possibly prevent cache misses. That are present in real life scenarios
    /// Shuffeling the keys first prevents this
    fn random_keys(&self) -> Vec<String> {
        let mut keys = self.random_keys_potentially_ordered();

        let mut rng = rand::thread_rng();
        keys.shuffle(&mut rng);

        keys
    }
}

impl RandomKeys for Vec<String> {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        (0..DESIRED_KEY_COUNT)
            .map(|_| {
                let index = random::<f64>() * self.len() as f64;
                self[index as usize].clone()
            })
            .collect()
    }
}

impl<T> RandomKeys for Vec<(String, T)> {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        (0..DESIRED_KEY_COUNT)
            .map(|_| {
                let index = random::<f64>() * self.len() as f64;
                self[index as usize].0.clone()
            })
            .collect()
    }
}

impl<T> RandomKeys for BTreeMap<String, T> {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        let mut v = Vec::with_capacity(DESIRED_KEY_COUNT);
        let chance = DESIRED_KEY_COUNT as f64 / self.len() as f64;
        v.extend(self.keys().filter(|_| random::<f64>() <= chance).cloned());
        v
    }
}

impl<T> RandomKeys for HashMap<String, T> {
    fn random_keys_potentially_ordered(&self) -> Vec<String> {
        let mut v = Vec::with_capacity(DESIRED_KEY_COUNT);
        let chance = DESIRED_KEY_COUNT as f64 / self.len() as f64;
        v.extend(self.keys().filter(|_| random::<f64>() <= chance).cloned());
        v
    }
}