:notoc:

.. Raphtory documentation master file, created by

.. module:: Raphtory

****************************************
Raphtory documentation
****************************************

**Date**: |today| **Version**: |version|

**Useful links**:
`Source Repository <https://github.com/Pometry/Raphtory>`__ |
`Issues & Ideas <https://github.com/Pometry/Raphtory/issues>`__ |
`Slack Support <https://slack.com/raphtory>`__

:mod:`Raphtory` is an in-memory graph tool written in Rust with friendly Python APIs on top.
It is blazingly fast, scales to hundreds of millions of edges on your laptop, and can be
dropped into your existing pipelines with a simple `pip install raphtory`.

.. grid:: 1 2 2 2
    :gutter: 4
    :padding: 2 2 0 0
    :class-container: sd-text-center

    .. grid-item-card:: Getting started
        :img-top: images/index_getting_started.svg
        :class-card: intro-card
        :shadow: md

        New to *Raphtory*? Check out the getting started guides. They contain an
        introduction to *Raphtory'* main concepts and links to additional tutorials.

        +++

        .. button-ref:: getting_started
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            To the getting started guides

    .. grid-item-card::  User guide
        :img-top: images/index_user_guide.svg
        :class-card: intro-card
        :shadow: md

        The user guide provides in-depth information on the
        key concepts of Raphtory with useful background information and explanation.

        +++

        .. button-ref:: user_guide
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            To the user guide

    .. grid-item-card::  API reference
        :img-top: images/index_api.svg
        :class-card: intro-card
        :shadow: md

        The reference guide contains a detailed description of
        the Raphtory API. The reference describes how the methods work and which parameters can
        be used. It assumes that you have an understanding of the key concepts.

        +++

        .. button-ref:: api
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            To the reference guide

    .. grid-item-card::  Developer guide
        :img-top: images/index_contribute.svg
        :class-card: intro-card
        :shadow: md

        Saw a typo in the documentation? Want to improve
        existing functionalities? The contributing guidelines will guide
        you through the process of improving Raphtory.

        +++

        .. button-ref:: development
            :ref-type: ref
            :click-parent:
            :color: secondary
            :expand:

            To the development guide


.. toctree::
    :maxdepth: 3
    :hidden:
    :titlesonly:


    getting_started/index
    development/index
    api/index
    userguide/index

