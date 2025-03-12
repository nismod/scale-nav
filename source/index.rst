.. scalenav documentation master file, created by
   sphinx-quickstart on Mon Oct 14 15:58:44 2024.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

scalenav
======================

A package for data manipulation using the H3 hierarchical indexed grid as base data schema.

.. tip::
   Refer to the H3 documentation for a better understanding of it's functionality. Or look into the guides of this package for our use case.

.. toctree::
   :caption: Startup
   :maxdepth: 1

   startup
   api/index

.. toctree::
   :caption: CL tool
   :titlesonly:
   :maxdepth: 1
   :glob:

   notebook/rastapar_doc
   .. notebook/data_ingestion

.. toctree::
   :caption: Guides
   :titlesonly:
   :maxdepth: 1
   :glob:

   notebook/h3_duckdb
   notebook/rescaling_h3_basics
   notebook/oop
   
.. toctree::
   :caption: Tutorials
   :titlesonly:
   :maxdepth: 1
   :glob:
   
   notebook/data_layer
   notebook/data_and_proxy_layer
   notebook/data_and_constraint_layer