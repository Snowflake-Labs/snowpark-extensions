# Snowpark Extensions

## Overview

The Snowpark Extensions project aims to simplify the migration process from Apache Spark to Snowpark for Scala developers. It provides a set of helper methods and utilities built as extensions on top of the existing Snowpark Scala APIs. 

The core goal is to minimize the amount of manual code changes required when migrating from Spark to Snowpark. This is achieved by leveraging Scala's implicit classes to essentially "overload" existing Snowpark classes like Column, DataFrame, and Session to have additional functionality not available out-of-the-box in Snowflake Snowpark Scala APIs.

## Features

The Snowpark Extensions project offers the following features:

- **Implicit Column Extensions** - Additional helper methods for Column to simplify common data transformation tasks
- **Implicit DataFrame Extensions** - Extra functionality for DataFrame to streamline migrations including things like improved join APIs
- **Implicit Session Extensions** - Helper utilities for Session to simplify setup and configuration
- ** SQL Extensions ** - Helper utilities that encapsulate functionality as UDFs
By leveraging implicits, these extensions provide overlayed APIs without requiring changes to existing Snowpark imports or references.

In some situations, some functions is easier to implement by registering some SQL or Javascript UDFs. You can find the code for some of them [at the scripts folder](https://github.com/Snowflake-Labs/snowpark-extensions/tree/main/scripts)

## Usage

To use the Snowpark Extensions project, simply import the extension classes:

```scala 
import com.snowflake.snowpark_extensions.Extensions._
```

This will bring all extended Column, DataFrame, and Session functionalities into scope. You can then utilize the additional methods as if they were available directly on the base classes.

## Building

The project uses Maven for building:

```
mvn clean compile package
```

This will compile the code and package it into a JAR file for distribution and dependency management.

The output JAR can then be included in any Scala application to leverage the Snowpark Extensions helpers.

## SQL Extensions

| UDF  | Description |
|---|---|
| conv | |
| format_string| |
| isnan | |
| nanvl | |
| substring_index | |


## Links

See the full API documentation here:
https://snowflake-labs.github.io/snowpark-extensions/
