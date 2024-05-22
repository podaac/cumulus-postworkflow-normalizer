# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security


## [0.4.0]

### Added
- ** DMRPP Updates **
  - Added a workflow flag to use ecs or lambda based on a granule size
  - Added a config argument for what the lambda ephemeral storage limit will be, 
    we will use that and subract 50 mb so we don't use the entire /tmp on lambdas.
- Updated to python 3.11
- Did 'poetry update' to update python libraries
- Updated to use arm architecture
### Changed
- ** Moved to Github.com **
  - [cumulus-postworkflow-normalizer](https://github.com/podaac/cumulus-postworkflow-normalizer)
### Deprecated
### Removed
### Fixed
### Security


## [0.3.0]

### Added
### Changed
- ** PODAAC-5948 **
  - python 3.9, cumulus-process-py 1.3.0
- ** Fix .bin data **
  - add .bin to exclusion file list
### Deprecated
### Removed
### Fixed
### Security


## [0.2.0]

### Added
- ** PODAAC-4790 **
  - Fix no type in files, check query fields to find data files 
### Changed
### Deprecated
### Removed
### Fixed
### Security


## [0.1.0]

### Added
- ** PODAAC-4732 **
  - Implementation of normalizing cumulus message for post workflow 
### Changed
### Deprecated
### Removed
### Fixed
### Security
