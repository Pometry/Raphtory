{{ header }}

.. _api:

=============
API reference
=============

This page gives an overview of all public raphtory objects, functions and
methods.

The following subpackages are public.

 - ``pandas.errors``: xyz


.. toctree::
   :maxdepth: 2

   io
   general_functions
   series
   frame
   arrays
   indexing
   offset_frequency
   window
   groupby
   resampling
   style
   plotting
   options
   extensions
   testing

.. This is to prevent warnings in the doc build. We don't want to encourage
.. these methods.

..
    .. toctree::

        api/pandas.Index.holds_integer
        api/pandas.Index.nlevels
        api/pandas.Index.sort


.. Can't convince sphinx to generate toctree for this class attribute.
.. So we do it manually to avoid a warning

..
    .. toctree::

        api/pandas.api.extensions.ExtensionDtype.na_value
