//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//




cfg_if! {
    if #[cfg(feature = "python_binding")] {
        use pravega_client::sync::Table;
        use tokio::runtime::Handle;
        use pyo3::prelude::*;
        use pyo3::PyResult;
        use pyo3::exceptions;
        use pravega_client::sync::table::Version;
        use pravega_client_retry::retry_async::retry_async;
        use pravega_client::sync::table::TableError;
        use pravega_client_retry::{retry_policy::RetryWithBackoff, retry_result::RetryResult};

    }
}


///
/// This represents a Key Value Table of counters. Each key maps to a counter.
/// Note: A python object of CountersTable cannot be created directly without using the StreamManager.
///
#[cfg(feature = "python_binding")]
#[pyclass]
pub(crate) struct CountersTable {
    table: Table,
    runtime_handle: Handle,
}


impl CountersTable {
    pub fn new(
        table: Table,
        runtime_handle: Handle,
    ) -> Self {
        CountersTable {
            table,
            runtime_handle,
        }
    }

    fn get_default_retry_policy(&self) -> RetryWithBackoff {
        let mut policy = RetryWithBackoff::default();
        policy = policy.backoff_coefficient(2);
        return policy        
    }

    async fn impl_get_value_async(&self, key: &str) -> Result<i64, TableError> {
        let nested_result = retry_async(self.get_default_retry_policy(), || async {
            let option_result:Result<Option<(i64, Version)>,TableError> = self.table.get(&key.to_string()).await;
            
            match option_result {
                Ok(Some((value, _version))) => {
                    return RetryResult::Success(Result::Ok(value));
                },
                Ok(None) => {
                    return RetryResult::Success(Result::Err(format!("Key {} does not exist", key)));
                },
                Err(e) => {
                    return RetryResult::Retry(e);
                }
            }
        
        })
        .await
        .map_err(|e| e.error);

        match nested_result {
            Ok(Ok(value)) => {
                return Ok(value)
            }
            Ok(Err(msg)) => {
                return Err(TableError::KeyDoesNotExist{
                    operation: format!("Get value for key {}.", key), 
                    error_msg: msg
                }); 
            }
            Err(e) => {
                return Err(e)
            }
        }
    }

    async fn impl_increment_async(&self, key: &str, increment_value: i64) -> Result<(), TableError>{
        let nested_res = retry_async(self.get_default_retry_policy(), || async {
            let option_result:Result<Option<(i64, Version)>,TableError> = self.table.get(&key.to_string()).await;
            
            match option_result {
                Ok(Some((value, version))) => {
                    let new_value = value + increment_value;
                    let insert_res = self.table.insert_conditionally(&key.to_string(), &new_value, version, -1).await;
                    match insert_res {
                        Ok(_) => {
                            return RetryResult::Success(Ok(()));
                        },
                        Err(e) => {
                            return RetryResult::Retry(e);
                        }
                    }
                },
                Ok(None) => {
                    return RetryResult::Success(Err(format!("Key {} does not exist", key)));
                }
                Err(e) => {
                    return RetryResult::Retry(e);
                }
            }
        })
        .await
        .map_err(|e| e.error);

        match nested_res {
            Ok(Ok(_)) => {
                return Ok(());
            }
            Ok(Err(msg)) => {
                return Err(TableError::KeyDoesNotExist{
                    operation: format!("Get value for key {}.", key), 
                    error_msg: msg
                }); 
            }
            Err(e) => {
                return Err(e)
            }
        }

    }

}

#[cfg(feature = "python_binding")]
#[pymethods]
impl CountersTable {
    #[pyo3(text_signature = "($self, key, init_value=0)")]
    #[args(key, init_value = 0, "*")]
    pub fn init_counter(&self, key: &str, init_value: i64) -> PyResult<()> {
        self.runtime_handle.block_on(async move {
            self.table.insert(&key.to_string(), &init_value, -1).await
        }).expect("Failed to put empty counter");
        Ok(())
    }



    #[pyo3(text_signature = "($self, key, increment_value=0)")]
    #[args(key, increment_value = 1, "*")]
    pub fn increment(&self, key: &str, increment_value: i64) ->PyResult<()> {
        let increment_result = self.runtime_handle.block_on(self.impl_increment_async(key, increment_value));
        
        match increment_result {
            Ok(_) => {
                Ok(())
            }
            Err(e) => Err(exceptions::PyOSError::new_err(format!(
                "Error while attempting to acquire segment {:?}",
                e
            ))),
        }
    }

    #[pyo3(text_signature = "($self, key, decrement_value=1)")]
    #[args(key, decrement_value = 1, "*")]
    pub fn decrement(&self, key: &str, decrement_value: i64) ->PyResult<()> {
        self.increment(key, -decrement_value)
    }


    pub fn get_value(&self, key: &str) -> PyResult<i64> {
        let value_result = self.runtime_handle.block_on(self.impl_get_value_async(key));
        
        match value_result {
            Ok(value) => {
                Ok(value)
            }
            Err(e) => Err(exceptions::PyOSError::new_err(format!(
                "Error while attempting to acquire segment {:?}",
                e
            ))),
        }

    }
}
