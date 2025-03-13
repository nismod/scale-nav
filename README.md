# Navigating spatial scales

![GitHub Actions Workflow
Status](https://img.shields.io/github/actions/workflow/status/nismod/scale-nav/run-package.yml?style=flat)
![PyPI -
Version](https://img.shields.io/pypi/v/scalenav?style=flat&logoColor=grey&logoSize=auto&labelColor=blue&color=gray&link=https%3A%2F%2Fpypi.org%2Fproject%2Fscalenav%2F)
![PyPI - Python
Version](https://img.shields.io/pypi/pyversions/scalenav?style=flat.png)
![PyPI - Wheel](https://img.shields.io/pypi/wheel/scalenav.png)

## Introduction

This is a python module that helps change scales of spatial data sets.
It relies on the [H3](https://h3geo.org) hexagonal grid’s hierarchical
indexing functionalities and aims to add to that the data
transformations that need to be done when navigating between levels of
the hierarchy.

A presentation on the topic can be found
<a href="https://ischlo.github.io/presentations/down_scaling"
target="_blank">here</a>

### THIS MODULE IS UNDER DEVELOPMENT

The package website introduces the use cases and the different functionalities that are covered so far.

## Main functionalities

### Rastapar

The command line tool allows for efficient, multi core ingestion of rasters into parquet tables. 

### Scalenav modules

A set of modules provides python functions for performing a set operations with data sets expressed as H3 cells. Additionaly, a module with the equivalent high performance functions written with ibis and duckdb. 
