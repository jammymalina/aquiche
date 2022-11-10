# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [1.2.0] - 2022-11-10

### Added

- Added a new cache parameter - `exit_stack_close_delay`
- Added 2 new functions - `cancel_exit_stack_close_operations` and `await_exit_stack_close_operations`
- Added a new option for `wrap_async_exit_stack` - `*` which wraps all attributes/items in the simple object or dict in the `AsyncExitStack`

### Changed

### Fixed

## [1.1.1] - 2022-11-06

### Added

- Added key parameter section to the README

### Changed

### Fixed

- Added a forgotten export for they Key enum to the init file

## [1.1.0] - 2022-11-06

### Added

- It is possible to set the key template

### Changed

- Removed the supported python versions badge from the README

### Fixed

## [1.0.3] - 2022-11-05

### Added

### Changed

- Updated poetry-core build system

### Fixed

- Fixed the README documentation

## [1.0.2] - 2022-11-05

### Added

### Changed

- Made the code blocks' lines in the README shorter so they do not overflow in PyPi page

### Fixed

- Fixed the cache stampede for the multithreaded scenarios

## [1.0.1] - 2022-11-05

### Added

- Added CHANGELOG.md

### Changed

### Fixed

- Fixed the cache stampede which was occurring due to the incorrect placement of the lock

## [1.0.0] - 2022-11-04

### Added

- Created the first version of the aquiche module

### Changed

### Fixed
