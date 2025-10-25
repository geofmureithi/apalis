# apalis-sql

SQL utilities for background job processing with apalis

## Overview
This crate contains basic utilities shared by different sql backed storages.

The following are the supported backends:
 - sqlite
 - postgres
 - mysql

 The following are under development
 - surreal db


 ## Usage

 You should no longer depend on this crate directly, the specific features have been moved eg for sqlite:
 ```toml
 [dependencies]
 apalis = "1"
 apalis-sqlite = "1"
 ```

 ## Licence

 MIT
