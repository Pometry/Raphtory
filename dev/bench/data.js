window.BENCHMARK_DATA = {
  "lastUpdate": 1694428772172,
  "repoUrl": "https://github.com/Pometry/Raphtory",
  "entries": {
    "Rust Benchmark": [
      {
        "commit": {
          "author": {
            "name": "Pometry",
            "username": "Pometry"
          },
          "committer": {
            "name": "Pometry",
            "username": "Pometry"
          },
          "id": "f375af6eed09c4968752c2dc830634224f622404",
          "message": "maybe new benchmark?",
          "timestamp": "2023-04-26T21:03:50Z",
          "url": "https://github.com/Pometry/Raphtory/pull/850/commits/f375af6eed09c4968752c2dc830634224f622404"
        },
        "date": 1682699241927,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 1036182,
            "range": "± 24881",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1059916,
            "range": "± 113090",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5e580fd81e12a894ed85c3e1ab37ea6b3a5fa9b3",
          "message": "Save new PRs into Master as Benchmark into a stats page (#850)\n\n* maybe new benchmark?\r\n\r\n* remove one bit\r\n\r\n* git ignored blocked me :<\r\n\r\n* add comment, add storage only on non master comit, add link to readme\r\n\r\n* bug\r\n\r\n* dont try save\r\n\r\n* dont try save",
          "timestamp": "2023-05-09T10:16:54+01:00",
          "tree_id": "2de8fdb80dcfbe52003fb20bcefa7d2dfaff1654",
          "url": "https://github.com/Pometry/Raphtory/commit/5e580fd81e12a894ed85c3e1ab37ea6b3a5fa9b3"
        },
        "date": 1683624135903,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 321649,
            "range": "± 21160",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 797919,
            "range": "± 168428",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b25e762960f0eef88b4604520a7a5bdc97398803",
          "message": "More Code Coverage (#847)\n\n* dont use pointer in loader\r\n\r\n* some extra code coverages\r\n\r\n* fix missing module, display tests\r\n\r\n* fix python graph loader\r\n\r\n* my family needs me\r\n\r\n* Update README.md\r\n\r\n* fix test, remove python from default, clean up warnings\r\n\r\n* fix url in pyproject\r\n\r\n* add retry to benchmark step\r\n\r\n* Update README.md\r\n\r\n* fix test issues",
          "timestamp": "2023-05-09T11:15:13+01:00",
          "tree_id": "83ea074e7cd2224a7d1ae36e137218240c3feacd",
          "url": "https://github.com/Pometry/Raphtory/commit/b25e762960f0eef88b4604520a7a5bdc97398803"
        },
        "date": 1683627579928,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 331068,
            "range": "± 1691",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 424743,
            "range": "± 26817",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "committer": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "distinct": true,
          "id": "ec3d1d6e46dc8931e712768184bd8505dc90cd30",
          "message": "forgot jobs",
          "timestamp": "2023-05-09T15:58:17+01:00",
          "tree_id": "3f859ae8531a8a2df9f70742def804942a51fa21",
          "url": "https://github.com/Pometry/Raphtory/commit/ec3d1d6e46dc8931e712768184bd8505dc90cd30"
        },
        "date": 1683644642012,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 353142,
            "range": "± 16634",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 864757,
            "range": "± 140182",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0188b6f3af0f6c122d2b90e0ee4a7f0eda517c3e",
          "message": "Dev 1654 post merge (#861)\n\n* use GraphViewOps instead\r\n\r\n* dead end probably\r\n\r\n* no dead end attempting an EvalGraph\r\n\r\n* EvalGraph don't work\r\n\r\n* no joy\r\n\r\n* revert from VertexViewOps\r\n\r\n* revert from VertexViewOps 3\r\n\r\n* revert rever revert\r\n\r\n* revert rever revert 2\r\n\r\n* minor fixes after rebase\r\n\r\n* bring back edge_refs\r\n\r\n* bring back edge_refs for dynamic graph too",
          "timestamp": "2023-05-09T16:13:23+01:00",
          "tree_id": "b66f113df2f42027c024eefcb0e458f6d9cc3a8a",
          "url": "https://github.com/Pometry/Raphtory/commit/0188b6f3af0f6c122d2b90e0ee4a7f0eda517c3e"
        },
        "date": 1683645531548,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 352033,
            "range": "± 4673",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 725752,
            "range": "± 112444",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "25484244+rachchan@users.noreply.github.com",
            "name": "Rachel Chan",
            "username": "rachchan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bc2a12f6559bb3537f66298838088ea5a8b1561e",
          "message": "add notebooks for companies house (#851)\n\n* add notebooks for companieshouse\r\n\r\n* add blog link\r\n\r\n* correct notebooks\r\n\r\n* updated notebook\r\n\r\n* updated notebook\r\n\r\n* finalise notebook\r\n\r\n* Delete aqWJlHS4_rJSJ7rLgTK49iO4gAg_0.json\r\n\r\n* added more markdown",
          "timestamp": "2023-05-09T16:21:48+01:00",
          "tree_id": "a8f8e22c4b95f20976905243b202eeb2aa9df902",
          "url": "https://github.com/Pometry/Raphtory/commit/bc2a12f6559bb3537f66298838088ea5a8b1561e"
        },
        "date": 1683646024399,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 279867,
            "range": "± 15899",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 558738,
            "range": "± 165142",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "committer": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "distinct": true,
          "id": "1bf63c5b48c358dee2f749aac94eb3f77bd70537",
          "message": "Merge remote-tracking branch 'origin/master'",
          "timestamp": "2023-05-09T16:35:28+01:00",
          "tree_id": "091e4dabfb58d40ad9d4f49b4a417bc727524ed7",
          "url": "https://github.com/Pometry/Raphtory/commit/1bf63c5b48c358dee2f749aac94eb3f77bd70537"
        },
        "date": 1683646845080,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 358534,
            "range": "± 2115",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 762097,
            "range": "± 115498",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "committer": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "distinct": true,
          "id": "6bde743fe7ab748365d57a28b2f1acf94cd2eb2e",
          "message": "Change WF permission and add PR",
          "timestamp": "2023-05-09T16:58:51+01:00",
          "tree_id": "c4c40b631fc790041df0ce0a586ea325cba6c6ec",
          "url": "https://github.com/Pometry/Raphtory/commit/6bde743fe7ab748365d57a28b2f1acf94cd2eb2e"
        },
        "date": 1683648218054,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 329370,
            "range": "± 1880",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 397900,
            "range": "± 70571",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "committer": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "distinct": true,
          "id": "210886cfae58ba8dade9530c9779931c15bea26f",
          "message": "remove bad needs",
          "timestamp": "2023-05-09T17:16:13+01:00",
          "tree_id": "c8700527a270fc3ed5b866556ad5a5ad9fccc7b6",
          "url": "https://github.com/Pometry/Raphtory/commit/210886cfae58ba8dade9530c9779931c15bea26f"
        },
        "date": 1683649292841,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 368951,
            "range": "± 10229",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 720762,
            "range": "± 94926",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bc41aff99b384ea975325e417e09dbf5e53f9355",
          "message": "add layer_name function to EdgeView and PyEdge (#864)\n\n* add layer_name function to EdgeView and PyEdge\r\n\r\n* add doc strings",
          "timestamp": "2023-05-09T17:46:47+01:00",
          "tree_id": "db34d4007f81fed2992cc8a0a97585996327218f",
          "url": "https://github.com/Pometry/Raphtory/commit/bc41aff99b384ea975325e417e09dbf5e53f9355"
        },
        "date": 1683650900239,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 339858,
            "range": "± 3572",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 393378,
            "range": "± 32809",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "afcbf0f4c448212771dfa2905485a7f555bf5ecc",
          "message": "Release v0.2.2 (#866)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-05-09T17:49:48+01:00",
          "tree_id": "992fc6da071eee9b726e51b44f2118d5d6bedd5b",
          "url": "https://github.com/Pometry/Raphtory/commit/afcbf0f4c448212771dfa2905485a7f555bf5ecc"
        },
        "date": 1683651327894,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 372520,
            "range": "± 2701",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 705451,
            "range": "± 113381",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0dcc128ae40daeed156a0c506757c8651ddb4153",
          "message": "Release v0.2.3 (#868)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-05-10T09:39:20+01:00",
          "tree_id": "688ffb666010deb493e7cd5c563f21b534ee4b6f",
          "url": "https://github.com/Pometry/Raphtory/commit/0dcc128ae40daeed156a0c506757c8651ddb4153"
        },
        "date": 1683708220092,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 332786,
            "range": "± 2969",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 415511,
            "range": "± 41069",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "cd5be06315cd359a22bba2a1c7379e54f515584c",
          "message": "Allow auto release  (#870)\n\n* rename to manual\r\n\r\n* rename to manual",
          "timestamp": "2023-05-10T10:28:50+01:00",
          "tree_id": "becf9143546ee5e299906e5a5e70621f60e0e555",
          "url": "https://github.com/Pometry/Raphtory/commit/cd5be06315cd359a22bba2a1c7379e54f515584c"
        },
        "date": 1683711014108,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 306248,
            "range": "± 1430",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 677341,
            "range": "± 98830",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2634c6ded19ef0c3c002aa70a88c86b56af6eb96",
          "message": "Release v0.2.4 (#871)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-05-10T10:43:23+01:00",
          "tree_id": "051bae8c6cc6c49c05f2c4a8df5e0da0588fe80b",
          "url": "https://github.com/Pometry/Raphtory/commit/2634c6ded19ef0c3c002aa70a88c86b56af6eb96"
        },
        "date": 1683712168166,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 372671,
            "range": "± 13734",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1077480,
            "range": "± 210700",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "da597d5ae1f2eb099de777f95a963622c6f41a21",
          "message": "Featurebug/pipeline revamp3 (#873)\n\n* rename to manual\r\n\r\n* undo manual",
          "timestamp": "2023-05-10T11:08:02+01:00",
          "tree_id": "be9443aa5352d68108917cf1ce43303756bf6a9a",
          "url": "https://github.com/Pometry/Raphtory/commit/da597d5ae1f2eb099de777f95a963622c6f41a21"
        },
        "date": 1683713635088,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 377781,
            "range": "± 6537",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 825170,
            "range": "± 114578",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "57c00d98a0b0837e7fb9e2487c18059e3019b3ca",
          "message": "Release v0.3.0 (#874)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-05-10T13:42:11+01:00",
          "tree_id": "bdc93d10f0bc1f49690791a424b9af78542efd66",
          "url": "https://github.com/Pometry/Raphtory/commit/57c00d98a0b0837e7fb9e2487c18059e3019b3ca"
        },
        "date": 1683722870655,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 367853,
            "range": "± 17557",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1013657,
            "range": "± 177883",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "91ae71cc6dcc368f446a4a38a6878fe682ce90b0",
          "message": "Release v0.3.1 (#875)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-05-10T14:00:25+01:00",
          "tree_id": "6f453961aa2009812dd67d74ae7bf93b68314069",
          "url": "https://github.com/Pometry/Raphtory/commit/91ae71cc6dcc368f446a4a38a6878fe682ce90b0"
        },
        "date": 1683723929842,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 337580,
            "range": "± 12892",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 816359,
            "range": "± 155186",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d1bbeee16ff36c83e3c47e9449df662128637b21",
          "message": "Dev 1654 pagerank (#869)\n\n* add dangling test\r\n\r\n* simplify page_rank and set initial value to 1\r\n\r\n* added initial support for dangling\r\n\r\n* remove println\r\n\r\n* a few more changes to match PageRank on old Raphtory\r\n\r\n* pagerank has the expected results",
          "timestamp": "2023-05-10T16:06:34+01:00",
          "tree_id": "5fff933b2efb54cb5347936cf9ba2c3e8bcabe9a",
          "url": "https://github.com/Pometry/Raphtory/commit/d1bbeee16ff36c83e3c47e9449df662128637b21"
        },
        "date": 1683731646633,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 312277,
            "range": "± 994",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 574641,
            "range": "± 82931",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "36d5656b40b217e664f0c93a91f1c822bb1bc5c1",
          "message": "add month/year support (#878)\n\n* add month/year support\r\n\r\n* add comments to clarify Add and Sub implementation",
          "timestamp": "2023-05-11T13:18:01+01:00",
          "tree_id": "3480300c06c6a918cbe3eb51907e52d5c4c36f6d",
          "url": "https://github.com/Pometry/Raphtory/commit/36d5656b40b217e664f0c93a91f1c822bb1bc5c1"
        },
        "date": 1683807792607,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 362755,
            "range": "± 12965",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 687588,
            "range": "± 114791",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f9b97250bec11d63d352d6352cc51aff80a5139b",
          "message": "Add time_index and generalize PyWindowSet (#884)\n\n* implement time_index for PyGraphWindowSet\r\n\r\n* add time_index to the rest of window sets\r\n\r\n* remove unused trait restriction\r\n\r\n* add dostring to time_index functions\r\n\r\n* fix compilation error\r\n\r\n* implement From for PyGenericIterator\r\n\r\n* use PyGenericIterator for PyGraphWindowSet\r\n\r\n* replace all python window sets with a new PyWindowSet struct\r\n\r\n* add IntoPyObject trait to avoid boilerplate\r\n\r\n* add custom name for PyWindowSet\r\n\r\n* simplify PyWindowSet struct\r\n\r\n* move time_index_impl inside the new PyWindowSet\r\n\r\n* fix compilation error\r\n\r\n* remove time_index_doc_string macro\r\n\r\n* remove unused imports\r\n\r\n* address comments",
          "timestamp": "2023-05-12T17:06:17+01:00",
          "tree_id": "1f7eecd70081b96d8e0865cac5158836b8d9185d",
          "url": "https://github.com/Pometry/Raphtory/commit/f9b97250bec11d63d352d6352cc51aff80a5139b"
        },
        "date": 1683907883960,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 357876,
            "range": "± 4597",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 688348,
            "range": "± 102097",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "25484244+rachchan@users.noreply.github.com",
            "name": "Rachel Chan",
            "username": "rachchan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3798473957d74e4ebf50b00e4fef52814966d0b1",
          "message": "Updated Companies House notebook and added new date time methods in python (#880)\n\n* updated notebook and added new date time methods in python\r\n\r\n* fix test\r\n\r\n* Delete utils.js\r\n\r\n* Delete tom-select.complete.min.js\r\n\r\n* Delete tom-select.css\r\n\r\n* Delete vis-network.css\r\n\r\n* Delete vis-network.min.js\r\n\r\n* Delete nx.html\r\n\r\n* Delete nx.html\r\n\r\n* remove file name",
          "timestamp": "2023-05-15T17:41:50+01:00",
          "tree_id": "e6ce6af500c2b2cfb23ad665195230f4c4a04975",
          "url": "https://github.com/Pometry/Raphtory/commit/3798473957d74e4ebf50b00e4fef52814966d0b1"
        },
        "date": 1684169197183,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 327457,
            "range": "± 2141",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 486597,
            "range": "± 63453",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "95bae46ccaff1143756c1158fda01a2c97ff2f3c",
          "message": "add support for datetime properties (#888)\n\nadd suport for datetime properties\r\n\r\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2023-05-15T17:56:29+01:00",
          "tree_id": "e707c3b15485627abbb898239e993bc159983ad5",
          "url": "https://github.com/Pometry/Raphtory/commit/95bae46ccaff1143756c1158fda01a2c97ff2f3c"
        },
        "date": 1684170089544,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 315446,
            "range": "± 13201",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 823160,
            "range": "± 191267",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "834a42cde5d092de58d42cbf92cec03a479e1fdd",
          "message": "fix bug edge function pointing always to default layer (#889)\n\n* fix bug edge function pointing always to default layer\r\n\r\n* port python test for unique layers over to rust\r\n\r\n* add nice comment to explain edge function implementation\r\n\r\n---------\r\n\r\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2023-05-15T18:19:23+01:00",
          "tree_id": "bee9ff61d7f55b1d448c4b80482c4391763fe6e0",
          "url": "https://github.com/Pometry/Raphtory/commit/834a42cde5d092de58d42cbf92cec03a479e1fdd"
        },
        "date": 1684171500489,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 362677,
            "range": "± 1086",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 895215,
            "range": "± 94222",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c013a96f99223559de2607eefb0f3a80fbba5d18",
          "message": "first take for wasm (#883)\n\n* first take for wasm\r\n\r\n* rename raphtory::graph_loader to raphtory_io::graph_loader\r\n\r\n* fix python module compilation issue\r\n\r\n* fix various tests that depend on csv add Graph is JS land\r\n\r\n* managed to add edges and vertices into raphtory from Javascript\r\n\r\n* actually push the wasm layer for graph\r\n\r\n* fix addVertex and addEdge to take either String or numbers\r\n\r\n* some sanity is revealed\r\n\r\n* attempt to return Vertex from rust when calling neighbours\r\n\r\n* neighbours finally works\r\n\r\n* break appart the various bits needed for wasm\r\n\r\n* added edges support and window\r\n\r\n* added a bit of extra logging to check why in_degree fails\r\n\r\n* added support for showing stack traces\r\n\r\n* add stuff back missed from the rebase\r\n\r\n* wasm POC works fine, next stop properties\r\n\r\n* remove the www submodule\r\n\r\n* add www as a folder not submodule\r\n\r\n* rename www to example\r\n\r\n* added properties for vertices and edges\r\n\r\n* move the benches into a separate project\r\n\r\n* setup benchmark workflor for raphtory-benchmark subproject\r\n\r\n* changes as per review\r\n\r\n* fix the docs for raphtory-io\r\n\r\n* fix the docs running the CsvLoader\r\n\r\n* Hello Raphtory in index.html",
          "timestamp": "2023-05-16T17:06:14+01:00",
          "tree_id": "63f2a707f64e87ce683eba490ed21cc6fbe49300",
          "url": "https://github.com/Pometry/Raphtory/commit/c013a96f99223559de2607eefb0f3a80fbba5d18"
        },
        "date": 1684253559132,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 334199,
            "range": "± 13303",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 613407,
            "range": "± 125556",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+iamsmkr@users.noreply.github.com",
            "name": "Shivam Kapoor",
            "username": "iamsmkr"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d621a57955536513bf1f239e41bf8aa1ce169f56",
          "message": "Gtnotebook (#882)\n\n* impl generic taint v2\r\n\r\n* fix merge compile/test issues\r\n\r\n* fix test\r\n\r\n* intro v2\r\n\r\n* impl hits with new apis\r\n\r\n* fix example\r\n\r\n* fix lotr\r\n\r\n* update ingestion\r\n\r\n* impl triplet count using new api\r\n\r\n* rid old triangle count impl\r\n\r\n* impl reciprocity using new api\r\n\r\n* impl cluster using new api\r\n\r\n* fix test",
          "timestamp": "2023-05-16T20:26:32+01:00",
          "tree_id": "2652d9197bd7ca790cb628a88aedb8fccb07442e",
          "url": "https://github.com/Pometry/Raphtory/commit/d621a57955536513bf1f239e41bf8aa1ce169f56"
        },
        "date": 1684265278672,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 336453,
            "range": "± 1710",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 401123,
            "range": "± 37325",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "dc8c387a64d2598ad06921f09a9e3568593f189a",
          "message": "Moved Fabians flux capacitor work over to the main repo (#896)\n\n* Moved Fabians flux capacitor work over to the main repo\r\n\r\n* fixes post rebase\r\n\r\n---------\r\n\r\nCo-authored-by: miratepuffin <b.a.steer@qmul.ac.uk>",
          "timestamp": "2023-05-16T22:37:54+01:00",
          "tree_id": "bb403b03b9fbfe5f1039f4674b834bae74b9199e",
          "url": "https://github.com/Pometry/Raphtory/commit/dc8c387a64d2598ad06921f09a9e3568593f189a"
        },
        "date": 1684273296801,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 317424,
            "range": "± 2296",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 778589,
            "range": "± 143067",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f0e5ed8d6137cf5477b8e2c71a8caee0fd25ff36",
          "message": "Feature/vertex view ops in eval (#897)\n\n* context needs to be public to use state api\r\n\r\n* Make EvalVertexView support VertexViewOps\r\n\r\n* implement EvalEdgeView properly and fix issues after rebase\r\n\r\n* add EdgeViewOps to view_api::*\r\n\r\n* change TimeIndex iterator to restore original api",
          "timestamp": "2023-05-17T16:09:32+02:00",
          "tree_id": "00ce6d4a0a3e9c21e0fb38e4d4266f5148ad506a",
          "url": "https://github.com/Pometry/Raphtory/commit/f0e5ed8d6137cf5477b8e2c71a8caee0fd25ff36"
        },
        "date": 1684332840105,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 359975,
            "range": "± 2528",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 831508,
            "range": "± 112627",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "796d906fbd92292d749d5cfcef20344bbb2659be",
          "message": "remove the need to declare a GraphViewInternalOps when proxying one (#898)\n\n* remove the need to declare a GraphViewInternalOps when proxying one\r\n\r\n* implement GraphViewInternalOps for any proxy\r\n\r\n---------\r\n\r\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2023-05-17T16:52:37+01:00",
          "tree_id": "0edc149b92149f31316e19ca3107cbbadecac42a",
          "url": "https://github.com/Pometry/Raphtory/commit/796d906fbd92292d749d5cfcef20344bbb2659be"
        },
        "date": 1684338976386,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 333957,
            "range": "± 2607",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 467458,
            "range": "± 68369",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33124479+narnolddd@users.noreply.github.com",
            "name": "Naomi Arnold",
            "username": "narnolddd"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a207f50cc4338ee38454f03a2700fb0194b959e9",
          "message": "Feature/temporal motifs (#876)\n\n* moved motifs to new raphtory repo\r\n\r\n* added so that accessible in python\r\n\r\n* removed ref to old API\r\n\r\n---------\r\n\r\nCo-authored-by: miratepuffin <b.a.steer@qmul.ac.uk>",
          "timestamp": "2023-05-17T18:01:05+01:00",
          "tree_id": "896a0f40b3a7b20d36114db90116306a3112bf56",
          "url": "https://github.com/Pometry/Raphtory/commit/a207f50cc4338ee38454f03a2700fb0194b959e9"
        },
        "date": 1684343084464,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 332260,
            "range": "± 2109",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 415070,
            "range": "± 81421",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+iamsmkr@users.noreply.github.com",
            "name": "Shivam Kapoor",
            "username": "iamsmkr"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "6063957d14f4907c31ee0f8dfb676f8c59b602a8",
          "message": "More stable coin graphs (#899)\n\nmore graphs",
          "timestamp": "2023-05-18T10:49:46+01:00",
          "tree_id": "f9c5e7fb874e3361f4b5f753c528d88e0def60cc",
          "url": "https://github.com/Pometry/Raphtory/commit/6063957d14f4907c31ee0f8dfb676f8c59b602a8"
        },
        "date": 1684403640041,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 366774,
            "range": "± 18388",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 780588,
            "range": "± 93260",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0b8cce84b52ebb5ed0c2d03095d5ca9198a4cd9c",
          "message": "Bug/vertex name (#901)\n\n* add test for consistent name\r\n\r\n* some benchmarks\r\n\r\n* more micro-benchmarks\r\n\r\n* make vertex look-up consistent independent of whether vertices were added using String or ID",
          "timestamp": "2023-05-19T10:14:46+02:00",
          "tree_id": "fe0f15c6a2ec95315e0f96b7b23d0b9708d99ec2",
          "url": "https://github.com/Pometry/Raphtory/commit/0b8cce84b52ebb5ed0c2d03095d5ca9198a4cd9c"
        },
        "date": 1684484323595,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 295426,
            "range": "± 717",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 841505,
            "range": "± 3056",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 888411,
            "range": "± 5573",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 678717,
            "range": "± 114942",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2065707,
            "range": "± 186831",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "51a9d5e46143d01494e3aeda8167d8608248f963",
          "message": "Bug/missing implementations in eval (#908)\n\n* finish implementing VertexView api for eval\r\n\r\n* Fix Vertex and Edge iterator methods and implement them for eval views\r\n\r\n* fix python and clean up warnings\r\n\r\n---------\r\n\r\nCo-authored-by: Shivam Kapoor <4599890+iamsmkr@users.noreply.github.com>",
          "timestamp": "2023-05-19T10:43:05+01:00",
          "tree_id": "6cdb7252ca361703af7425e80a646e1c98d6ad3d",
          "url": "https://github.com/Pometry/Raphtory/commit/51a9d5e46143d01494e3aeda8167d8608248f963"
        },
        "date": 1684489616611,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 294720,
            "range": "± 1115",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 855982,
            "range": "± 2758",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 901569,
            "range": "± 4021",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 715464,
            "range": "± 139100",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2038609,
            "range": "± 164684",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+iamsmkr@users.noreply.github.com",
            "name": "Shivam Kapoor",
            "username": "iamsmkr"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e6f99e74deb20beabd53eec9b3c1be66f30c6e07",
          "message": "Stablecoin nb (#909)\n\n* more graphs\r\n\r\n* impl motif using new api\r\n\r\n* restruct modules",
          "timestamp": "2023-05-19T16:33:23+01:00",
          "tree_id": "b1135e97e0850874303175ee28288ee6b659efdf",
          "url": "https://github.com/Pometry/Raphtory/commit/e6f99e74deb20beabd53eec9b3c1be66f30c6e07"
        },
        "date": 1684510670919,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 371275,
            "range": "± 2664",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1002359,
            "range": "± 10382",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1052155,
            "range": "± 17724",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 763937,
            "range": "± 104897",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2491265,
            "range": "± 150852",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "985881cba2a1640ea98874210b966fefdf82f743",
          "message": "Switch to dynamic-graphql (#912)\n\n* hello world\r\n\r\n* basic window\r\n\r\n* switch to ResolvedObject\r\n\r\n* complete implementation for the rest of the objects\r\n\r\n* add pagerank\r\n\r\n* add separate algorithms.rs file to model module\r\n\r\n* cleanup\r\n\r\n* remove commented out code\r\n\r\n* re-establish dynamic-graphql version\r\n\r\n* remove dangling code\r\n\r\n* wip\r\n\r\n* make threads and tol args optional for pagerank\r\n\r\n* rename PageRank to Pagerank\r\n\r\n* change port to 1736\r\n\r\n* remove example algorithm",
          "timestamp": "2023-05-19T17:35:16+01:00",
          "tree_id": "993ed2c7e8e0883b8997c05039f723fca452b9b2",
          "url": "https://github.com/Pometry/Raphtory/commit/985881cba2a1640ea98874210b966fefdf82f743"
        },
        "date": 1684514350465,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 314693,
            "range": "± 714",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 851324,
            "range": "± 6492",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 885350,
            "range": "± 2870",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 651864,
            "range": "± 110884",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2105018,
            "range": "± 147069",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "76895047ef4f429fad2d42fc92ff5468f804aa0a",
          "message": "Feature/api improvements (#961)\n\n* make python wrappers useable outside of crate\r\n\r\n* small eval improvements\r\n\r\n* return the pyvis graph for further tweaks\r\n\r\n* refactor python bindings to make exposing them possible\r\n\r\n* fix imports (maybe we should move algirhtms)\r\n\r\n* make all the things public\r\n\r\n* move the tests to the correct place\r\n\r\n* ignore all the python generated things\r\n\r\n* fix workflows and improve naming",
          "timestamp": "2023-05-22T17:53:00+01:00",
          "tree_id": "d813ea809bcbc4f3bdc746947dc9a43dd17dd249",
          "url": "https://github.com/Pometry/Raphtory/commit/76895047ef4f429fad2d42fc92ff5468f804aa0a"
        },
        "date": 1684774614828,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 313772,
            "range": "± 1220",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 864692,
            "range": "± 9218",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 901498,
            "range": "± 16894",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 725371,
            "range": "± 124056",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2250769,
            "range": "± 129302",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e94fd1bbc6d0c3647e8abd930d5046b64b87aa27",
          "message": "Bump requests from 2.28.2 to 2.31.0 (#967)\n\nBumps [requests](https://github.com/psf/requests) from 2.28.2 to 2.31.0.\r\n- [Release notes](https://github.com/psf/requests/releases)\r\n- [Changelog](https://github.com/psf/requests/blob/main/HISTORY.md)\r\n- [Commits](https://github.com/psf/requests/compare/v2.28.2...v2.31.0)\r\n\r\n---\r\nupdated-dependencies:\r\n- dependency-name: requests\r\n  dependency-type: direct:production\r\n...",
          "timestamp": "2023-05-23T09:30:27+01:00",
          "tree_id": "27de67346cb9c5ffc242aa8a965df551a33f5761",
          "url": "https://github.com/Pometry/Raphtory/commit/e94fd1bbc6d0c3647e8abd930d5046b64b87aa27"
        },
        "date": 1684830906472,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 336365,
            "range": "± 7941",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 983161,
            "range": "± 57529",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1042297,
            "range": "± 28814",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 843690,
            "range": "± 200626",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2542226,
            "range": "± 242432",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5c587d314a790d60571b9ec57d75ec7dc8389b5a",
          "message": "fix Dockerfile (#969)\n\n* fix Dockerfile\r\n\r\n* update .dockerignore\r\n\r\n* add newline to .dockerignore\r\n\r\n* rename .env/ to .env in .dockerignore",
          "timestamp": "2023-05-23T11:21:48+01:00",
          "tree_id": "2f78f125ca3689cfece77cb9722040b7c2a16ff1",
          "url": "https://github.com/Pometry/Raphtory/commit/5c587d314a790d60571b9ec57d75ec7dc8389b5a"
        },
        "date": 1684837533774,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 298251,
            "range": "± 5375",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 848018,
            "range": "± 8135",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 883665,
            "range": "± 3781",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 596262,
            "range": "± 103864",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2001373,
            "range": "± 120975",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b88df921fe60b7e2b94d7a0362a64e20062290a9",
          "message": "Feature/graph ception (#972)\n\n* add a graph as a property\r\n\r\n* Make Graph wrap an Arc of the actual graph for extra-cheap clone\r\n\r\n* need Display\r\n\r\n* make the Graph wrap an Arc over the shards for hopefully faster clone",
          "timestamp": "2023-05-24T11:36:09+02:00",
          "tree_id": "3eff87a5450296530e0f83b9f259554df0714e50",
          "url": "https://github.com/Pometry/Raphtory/commit/b88df921fe60b7e2b94d7a0362a64e20062290a9"
        },
        "date": 1684921199462,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 294956,
            "range": "± 1202",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 855380,
            "range": "± 6357",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 895113,
            "range": "± 15120",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 699476,
            "range": "± 100373",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2163949,
            "range": "± 121129",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "25484244+rachchan@users.noreply.github.com",
            "name": "Rachel Chan",
            "username": "rachchan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ac2c865e5b4652a929eca4270ea009cc0e35592b",
          "message": "More methods for UI (#974)\n\nmore methods for ui",
          "timestamp": "2023-05-25T12:47:41+01:00",
          "tree_id": "71b20cd912ab0da9285ce2082963f5e5c3f6a5ed",
          "url": "https://github.com/Pometry/Raphtory/commit/ac2c865e5b4652a929eca4270ea009cc0e35592b"
        },
        "date": 1685015525932,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 301585,
            "range": "± 856",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 861572,
            "range": "± 1253",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 901458,
            "range": "± 1536",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 765430,
            "range": "± 98375",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2262042,
            "range": "± 104349",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "76da906de9a4511468026d89444378dc45faf615",
          "message": "make graphql server load all the graphs under graphs folder (#971)\n\n* make graphql server load all the graphs under graphs folder\r\n\r\n* replace Dockerfile with docker-compose.yml\r\n\r\n* try removing  --no-default-features from raphtory-pymodule\r\n\r\n* revert back changes in cargo config files\r\n\r\n* remove commented out code in data.rs\r\n\r\n* Use Arc instead of cloning graphs\r\n\r\n* turn back on --no-default-features flag for code coverage\r\n\r\n* remove line in Cargo.toml\r\n\r\n* add README.md to docker example\r\n\r\n* add example data to .gitignore\r\n\r\n* make versions depend on root Cargo.toml\r\n\r\n* make python/Cargo.toml rely on the root for the version number\r\n\r\n* add versions to local dependencies for raphtory-graphql\r\n\r\n---------\r\n\r\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2023-05-25T13:08:00+01:00",
          "tree_id": "2b61ef641f283367e0861eb23a0e821bc0b85028",
          "url": "https://github.com/Pometry/Raphtory/commit/76da906de9a4511468026d89444378dc45faf615"
        },
        "date": 1685016716103,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 301024,
            "range": "± 739",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 862397,
            "range": "± 3065",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 903845,
            "range": "± 3203",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 719927,
            "range": "± 106775",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2196730,
            "range": "± 126634",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b544bf4e11444fdc72aa9dfd8294652427c5c479",
          "message": "Add edge id as tuple of src and dst id and clean up temporary vectors (#970)\n\n* Add edge id as tuple of src and dst id and clean up temporary vectors\r\n\r\n* make edge.explode() return edges object in python so we have vectorised access to properties/etc.\r\n\r\n* fix active for exploded edge\r\n\r\n* fix earliest and latest time for exploded edge\r\n\r\n* add test to check sanity of exploded edges\r\n\r\n* exploded edge test fixes\r\n\r\n* add FIXME note for exploded edges\r\n\r\n---------\r\n\r\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2023-05-25T14:47:16+01:00",
          "tree_id": "c96df2e7ef531e30f93ef9052417da178802e328",
          "url": "https://github.com/Pometry/Raphtory/commit/b544bf4e11444fdc72aa9dfd8294652427c5c479"
        },
        "date": 1685022561465,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 416385,
            "range": "± 94406",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1196171,
            "range": "± 51615",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1229249,
            "range": "± 108261",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1094436,
            "range": "± 224397",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2835308,
            "range": "± 369724",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bc110d75c4598c7bb4293d36355f1a85cce7284d",
          "message": "publish py-raphtory, raphtory-io and raphtory-graphql into crates wit… (#980)\n\npublish py-raphtory, raphtory-io and raphtory-graphql into crates with CI/CD",
          "timestamp": "2023-05-25T17:02:34+01:00",
          "tree_id": "160f9abce736332fa0e2a693484bfb23deaea8f2",
          "url": "https://github.com/Pometry/Raphtory/commit/bc110d75c4598c7bb4293d36355f1a85cce7284d"
        },
        "date": 1685030672758,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 369066,
            "range": "± 24261",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1069527,
            "range": "± 24798",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1092897,
            "range": "± 26491",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1016808,
            "range": "± 209861",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2997018,
            "range": "± 201885",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1aed920c7faee96ff599fd130301fb564f977a10",
          "message": "Release v0.3.2 (#981)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-05-25T17:11:04+01:00",
          "tree_id": "82520ba5907c83f71c7b47bc55f6b70ecc12cf13",
          "url": "https://github.com/Pometry/Raphtory/commit/1aed920c7faee96ff599fd130301fb564f977a10"
        },
        "date": 1685031412584,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 401533,
            "range": "± 22209",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1184287,
            "range": "± 233706",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1224079,
            "range": "± 35794",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1063155,
            "range": "± 232947",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2919627,
            "range": "± 397107",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "committer": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "distinct": true,
          "id": "6d564fb2056239cc462276ac4dd37f04e446aef5",
          "message": "update lockfile",
          "timestamp": "2023-05-26T11:38:48+01:00",
          "tree_id": "800fb992a87f30399b2279709bb1f2a236a2af4f",
          "url": "https://github.com/Pometry/Raphtory/commit/6d564fb2056239cc462276ac4dd37f04e446aef5"
        },
        "date": 1685097768298,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 341155,
            "range": "± 2086",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 934442,
            "range": "± 10535",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 966056,
            "range": "± 2102",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 419930,
            "range": "± 99978",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1748538,
            "range": "± 43116",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fa538349143ebf0a44623c86957cca4a033d03f4",
          "message": "Freature/property access improvements (#979)\n\n* add conversion methods for different property types\r\n\r\n* easy unwrapping of property values in rust",
          "timestamp": "2023-05-26T13:01:26+01:00",
          "tree_id": "0f1a52453f25dbf22deb7e01e604d3ccfed89e74",
          "url": "https://github.com/Pometry/Raphtory/commit/fa538349143ebf0a44623c86957cca4a033d03f4"
        },
        "date": 1685102721670,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 315723,
            "range": "± 729",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 879059,
            "range": "± 43439",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 916846,
            "range": "± 3036",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 605447,
            "range": "± 101346",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2020634,
            "range": "± 150622",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "dcf8c329b176da8473066fff297d1cafdea2726b",
          "message": "delete cargo file before runing benchmark commit (#993)",
          "timestamp": "2023-05-29T11:21:06+01:00",
          "tree_id": "6bb63cb6511605d8380d50b8b8092b181dffc0d9",
          "url": "https://github.com/Pometry/Raphtory/commit/dcf8c329b176da8473066fff297d1cafdea2726b"
        },
        "date": 1685355897699,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 329429,
            "range": "± 2969",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 947118,
            "range": "± 5632",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 964268,
            "range": "± 14489",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 414575,
            "range": "± 105268",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1761273,
            "range": "± 54331",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8a754b8cbf4c48d585bcf14beda9076edd2bc1e9",
          "message": "Pagerankv2 (#978)\n\n* move pagerank to f64 and relax the convergence\r\n\r\n* completely revamp local state\r\n\r\n* completely revamp local state - add prev_local_state\r\n\r\n* confirm we can set the local state from ATask\r\n\r\n* need to define EvalVertexState and make the vector of local states fixed size to address quickly\r\n\r\n* setup for the state is ready\r\n\r\n* it finally compiles\r\n\r\n* actually add the verte state\r\n\r\n* pull the local state out of EVState to avoid the Rc cost\r\n\r\n* began testing pull vesion of pagerank\r\n\r\n* found a bug in max_diff but pagerank first iteration shows up the right result\r\n\r\n* fix state update\r\n\r\n* remove useless clone\r\n\r\n* fixed all the types to f64, results are correct but normalisation fails\r\n\r\n* pagerank not working out yet\r\n\r\n* pagerank works now\r\n\r\n* all pagerank tests pass\r\n\r\n* added generic state S to the task eval\r\n\r\n* pagerank is at 4s\r\n\r\n* ImmutableGraph is now GraphViewInternalOps\r\n\r\n* erase lifetimes with macros\r\n\r\n* various attempts at improving speed\r\n\r\n* pagerank at 1.3s\r\n\r\n* minor change before rebase\r\n\r\n* fix issues after rebase\r\n\r\n* setting up to support window on EvalVertexView\r\n\r\n* Only WindowEvalEdgeView left\r\n\r\n* still loads of todos but we compile, need to adapt algos next\r\n\r\n* hits now works\r\n\r\n* generic taint works\r\n\r\n* all algos work appart from node motifs which didn't work in parallel before\r\n\r\n* mostly done\r\n\r\n* removed some warnings and bitset\r\n\r\n* fix the python test\r\n\r\n---------\r\n\r\nCo-authored-by: Lucas Jeub <lucas.jeub@pometry.com>",
          "timestamp": "2023-05-30T14:08:45+02:00",
          "tree_id": "73f7cb4c2a0aa07b399ef1094953e0837f3b4eb7",
          "url": "https://github.com/Pometry/Raphtory/commit/8a754b8cbf4c48d585bcf14beda9076edd2bc1e9"
        },
        "date": 1685448767871,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 338395,
            "range": "± 1356",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 950581,
            "range": "± 1371",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 976917,
            "range": "± 1971",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 453579,
            "range": "± 80056",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1789969,
            "range": "± 61174",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d7dfc32d91fb1fce14ffe12eef344b3e7a5f837c",
          "message": "Benchmark Suite v1 (#977)\n\n* memmy benchy\r\n\r\n* graph_Tool\r\n\r\n* networkx\r\n\r\n* cozo start, annoying\r\n\r\n* checkin\r\n\r\n* raphtory bench\r\n\r\n* kuzu bench\r\n\r\n* neo4j bench\r\n\r\n* fix out neighbours no ids\r\n\r\n* return all results\r\n\r\n* memgraph bench\r\n\r\n* add driver\r\n\r\n* add driver\r\n\r\n* use complex rel file\r\n\r\n* set shards to cpu count\r\n\r\n* fix n x bench\r\n\r\n* fix n x bench\r\n\r\n* fix n x bench\r\n\r\n* add cozo bench\r\n\r\n* add names\r\n\r\n* add pp result when complete\r\n\r\n* allow running all benchmarks\r\n\r\n* add download and unzip data files\r\n\r\n* fix graph tool and point to correct file for all benches\r\n\r\n* add close() fn to all benches to release graph resources and delete graph object\r\n\r\n* add arg selection to driver, add docker option but no implementation, fix cozo bench dir issue\r\n\r\n* initial docker test but its fialing on some xcode issue\r\n\r\n* benchmark with cozo and docker complete\r\n\r\n* better docker bench\r\n\r\n* networkx with docker\r\n\r\n* raphtory docker\r\n\r\n* i dont know why my neo bench gets stuck\r\n\r\n* add kuzu docker\r\n\r\n* graph tool potentially done?\r\n\r\n* graph tool done\r\n\r\n* memgraph done, use internal csv loader instead\r\n\r\n* fix memgraph version\r\n\r\n* fix neo benchmark\r\n\r\n* benchmark complete\r\n\r\n* enable docker by default, add start of readme\r\n\r\n* everything works except memgraph :<\r\n\r\n* memgraph now works,\r\n\r\n* benchmark complete\r\n\r\n* move folder\r\n\r\n* add better template\r\n\r\n* cleanup soft\r\n\r\n* fix outside name\r\n\r\n* dont fail if not locally installed\r\n\r\n* fix cozo import too quick\r\n\r\n* run all\r\n\r\n* fix networkx hang\r\n\r\n* remove ugly not found, maybe fix memgraph?\r\n\r\n* use z mode for volume\r\n\r\n* remove tqdm in raphtory\r\n\r\n* Fixed call to pagerank\r\n\r\n---------\r\n\r\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2023-05-30T15:40:04+01:00",
          "tree_id": "49255babe07ea12d9ff3594c9365cc79faa91c5b",
          "url": "https://github.com/Pometry/Raphtory/commit/d7dfc32d91fb1fce14ffe12eef344b3e7a5f837c"
        },
        "date": 1685457869647,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 354072,
            "range": "± 10266",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1038738,
            "range": "± 49185",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1077906,
            "range": "± 19305",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 735298,
            "range": "± 114484",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2450058,
            "range": "± 182358",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b8daedc666312735bbf6c6cc3de9529b461a423b",
          "message": "new docs (#994)\n\n* new docs\r\n\r\n* finished getting started, added intro guide, added lotr ipynb, finished development guide with many pages, added initial api\r\n\r\n* added modules as docs\r\n\r\n* fix reqs\r\n\r\n* fix pands version\r\n\r\n* numpydoc\r\n\r\n* numpydoc\r\n\r\n* fix api doc to use google format\r\n\r\n* docs strings all expand now\r\n\r\n* move images to their own folder\r\n\r\n* move images to their own folder\r\n\r\n* move images to their own folder\r\n\r\n* fixed colors\r\n\r\n* add license and clean user guide\r\n\r\n* reqs\r\n\r\n* fix reqs for test\r\n\r\n* install raph into docs",
          "timestamp": "2023-05-31T13:48:58+01:00",
          "tree_id": "6d5b23fb931bb7d73afdb4e9cceb2c945a1dc991",
          "url": "https://github.com/Pometry/Raphtory/commit/b8daedc666312735bbf6c6cc3de9529b461a423b"
        },
        "date": 1685537580694,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 309557,
            "range": "± 831",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 880385,
            "range": "± 2009",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 921948,
            "range": "± 3821",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 751409,
            "range": "± 102691",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2272238,
            "range": "± 109204",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e0289d05a2b305e286c14eea1c7189ce15fae6a7",
          "message": "Feature/connected components 2.0 (#995)\n\n* Swapped cc to local state\r\n\r\n* Fixed my busted implmenetation",
          "timestamp": "2023-05-31T17:27:27+01:00",
          "tree_id": "eeef413ea20c03de4ce5b2fe6968fc808052b3e4",
          "url": "https://github.com/Pometry/Raphtory/commit/e0289d05a2b305e286c14eea1c7189ce15fae6a7"
        },
        "date": 1685550551609,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 296875,
            "range": "± 1392",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 877395,
            "range": "± 6123",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 916550,
            "range": "± 5176",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 684439,
            "range": "± 131095",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2059399,
            "range": "± 163118",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "37c403630a9ff4f951198b75655878ccee7e0784",
          "message": "add initial plugin example (#998)\n\n* add initial plugin example\r\n\r\n* revert back testing changes\r\n\r\n* clean up changes\r\n\r\n* fix Dockerfile",
          "timestamp": "2023-06-01T11:20:27+01:00",
          "tree_id": "69bb6a8bb8a775dfe422916a3eda28a4d03f7d08",
          "url": "https://github.com/Pometry/Raphtory/commit/37c403630a9ff4f951198b75655878ccee7e0784"
        },
        "date": 1685615063107,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 316461,
            "range": "± 2415",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 896054,
            "range": "± 3610",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 935646,
            "range": "± 3009",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 725418,
            "range": "± 109748",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2217248,
            "range": "± 137905",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1f31f0668c89b9ce0609cb90f19c33db24ee474f",
          "message": "added algos back into docs (#1008)",
          "timestamp": "2023-06-01T16:22:52+01:00",
          "tree_id": "aec4d1f01c2cdc60783cdcf45c857c0a876e26f7",
          "url": "https://github.com/Pometry/Raphtory/commit/1f31f0668c89b9ce0609cb90f19c33db24ee474f"
        },
        "date": 1685633071697,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 337273,
            "range": "± 2845",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 945770,
            "range": "± 15128",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 984493,
            "range": "± 2577",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 491292,
            "range": "± 30332",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1922813,
            "range": "± 71793",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0f9f70e84b36d8b598bcc6e9528240a577fdb9f1",
          "message": "remove duplicate method and fix local state for windowed view (#1004)\n\n* remove duplicate method and fix local state for windowed view\r\n\r\n* len is max id + 1!",
          "timestamp": "2023-06-01T19:30:22+01:00",
          "tree_id": "6f7ffe6b1a9d2382beb631f34b0767f7f3edde48",
          "url": "https://github.com/Pometry/Raphtory/commit/0f9f70e84b36d8b598bcc6e9528240a577fdb9f1"
        },
        "date": 1685644324655,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 336932,
            "range": "± 1952",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 945226,
            "range": "± 6324",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1008074,
            "range": "± 5553",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 444232,
            "range": "± 79625",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1808090,
            "range": "± 59567",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "57d845c552cfe8d35e420385ccbaa44ffa948aaa",
          "message": "Fixing the ingestion of the stable coin data (#1011)\n\n* Fixed the ingestion of the stable coin data - just need to manage the zip extraction\r\n\r\n* Added unzipping to raphtory-io\r\n\r\n* Notebook fixed\r\n\r\n* Turn off taint test\r\n\r\n* Missing arg\r\n\r\n* Fixed comment example",
          "timestamp": "2023-06-02T18:46:36+01:00",
          "tree_id": "95fd955b2454302375df445bc6bf78913238f56e",
          "url": "https://github.com/Pometry/Raphtory/commit/57d845c552cfe8d35e420385ccbaa44ffa948aaa"
        },
        "date": 1685728230644,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 312078,
            "range": "± 566",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 866275,
            "range": "± 7782",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 909588,
            "range": "± 14944",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 705851,
            "range": "± 88309",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2202899,
            "range": "± 135357",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "bf04be552f9a4394846209c6218688a5ee765847",
          "message": "Adding IOT data for the going meta example (#1015)\n\n* Minor fixes for notebooks\r\n\r\n* ingesting base data\r\n\r\n* Changed add_vertex to accept strings\r\n\r\n* Finished initial version\r\n\r\n* Tidied some comments\r\n\r\n* Adding going meta data",
          "timestamp": "2023-06-04T10:19:27+01:00",
          "tree_id": "89083795719e9fabe091cf00ecad4e1a95c33f17",
          "url": "https://github.com/Pometry/Raphtory/commit/bf04be552f9a4394846209c6218688a5ee765847"
        },
        "date": 1685870619492,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 298072,
            "range": "± 1009",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 855929,
            "range": "± 1925",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 897157,
            "range": "± 3591",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 647678,
            "range": "± 98024",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2051775,
            "range": "± 145610",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "74d6340701c4a25dfbe87c529cdac0f689af953d",
          "message": "Add Vertex and Edges to docs (#1020)\n\ninitial nx ones",
          "timestamp": "2023-06-05T22:30:05+01:00",
          "tree_id": "d29496f95ebb04d66933bcc6b40ed10db519ae4c",
          "url": "https://github.com/Pometry/Raphtory/commit/74d6340701c4a25dfbe87c529cdac0f689af953d"
        },
        "date": 1686000835433,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 293586,
            "range": "± 840",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 865615,
            "range": "± 1349",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 903811,
            "range": "± 3328",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 613929,
            "range": "± 98535",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2062828,
            "range": "± 132962",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0cfde833547737127f59550e863b90942b67d5d5",
          "message": "Multiple Benchmarks (#1009)\n\n* trying to fix neo4j\r\n\r\n* nearly there but its mad slow\r\n\r\n* nearly there but its mad slow\r\n\r\n* results table\r\n\r\n* only modify if start docker\r\n\r\n* added grap tool bench\r\n\r\n* add raphtory bench\r\n\r\n* kzu timesout\r\n\r\n* dont pull much data from kuzu\r\n\r\n* dont pull much data from kuzu\r\n\r\n* kuzu done\r\n\r\n* Add cozo and key notes\r\n\r\n* added more neo notes\r\n\r\n* correction\r\n\r\n* memgraph potentially load CSV nodelist and create index to improve speed?\r\n\r\n* memgraph potentially load CSV nodelist and create index to improve speed?\r\n\r\n* slowgraph added\r\n\r\n* initial nx ones",
          "timestamp": "2023-06-06T12:04:13+01:00",
          "tree_id": "71dfcfa47a155ebf3698636d13972f8250ef9a7a",
          "url": "https://github.com/Pometry/Raphtory/commit/0cfde833547737127f59550e863b90942b67d5d5"
        },
        "date": 1686049690799,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 300278,
            "range": "± 1062",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 857199,
            "range": "± 21234",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 899464,
            "range": "± 3410",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 752440,
            "range": "± 128363",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2285089,
            "range": "± 129210",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d78469cddbcb7284e20591dc33a9860cd676faff",
          "message": "Extract time works with python date time obj (#1021)\n\n* add datetime to add vertex\r\n\r\n* remove unused imports",
          "timestamp": "2023-06-06T15:00:34+01:00",
          "tree_id": "deeece722bd4393d0959c80a37dc33cfa2c5b0c3",
          "url": "https://github.com/Pometry/Raphtory/commit/d78469cddbcb7284e20591dc33a9860cd676faff"
        },
        "date": 1686060275985,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 303233,
            "range": "± 833",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 865614,
            "range": "± 1499",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 915015,
            "range": "± 4739",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 733701,
            "range": "± 112078",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2178507,
            "range": "± 140883",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "70bb3e52449ddb886564635f9b7eed91867c6fe1",
          "message": "added early json loader (#1022)",
          "timestamp": "2023-06-06T16:03:46+01:00",
          "tree_id": "0e6fd45156f5fe4c44192f6e6be51e3167639250",
          "url": "https://github.com/Pometry/Raphtory/commit/70bb3e52449ddb886564635f9b7eed91867c6fe1"
        },
        "date": 1686064071208,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 297196,
            "range": "± 819",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 854010,
            "range": "± 3495",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 893194,
            "range": "± 3754",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 771830,
            "range": "± 133122",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2309186,
            "range": "± 165110",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+iamsmkr@users.noreply.github.com",
            "name": "Shivam Kapoor",
            "username": "iamsmkr"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c1a0f1da1b8cb17c5a95397d19b3bf18415f91f4",
          "message": "Lotrgql (#1023)\n\n* impl edges gql\r\n\r\n* company house data\r\n\r\n* add properties and project wide fmt\r\n\r\n* impl neoghbours\r\n\r\n* adding flag\r\n\r\n* add inneighs\r\n\r\n* merge from master\r\n\r\n* ignore test\r\n\r\n---------\r\n\r\nCo-authored-by: RachelChan <25484244+rachchan@users.noreply.github.com>",
          "timestamp": "2023-06-07T11:41:35+01:00",
          "tree_id": "2e9031307a72129d46638aad0e4a657c641a0b3a",
          "url": "https://github.com/Pometry/Raphtory/commit/c1a0f1da1b8cb17c5a95397d19b3bf18415f91f4"
        },
        "date": 1686134740223,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 329603,
            "range": "± 1635",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 907569,
            "range": "± 3831",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 939096,
            "range": "± 3653",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 546376,
            "range": "± 42731",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1943140,
            "range": "± 63813",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "71fe58360e76fda7b60c04c83a91b0a8faeda1c5",
          "message": "Fixed small bug where .ds_store was attempted to be read as a graph (#1024)",
          "timestamp": "2023-06-07T14:53:53+01:00",
          "tree_id": "75fb50dff2d3492bc99faf9857de9b35f7c8222a",
          "url": "https://github.com/Pometry/Raphtory/commit/71fe58360e76fda7b60c04c83a91b0a8faeda1c5"
        },
        "date": 1686146278734,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 295201,
            "range": "± 470",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 859426,
            "range": "± 3335",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 900897,
            "range": "± 2927",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 719306,
            "range": "± 133386",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2218674,
            "range": "± 175560",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "07354ba9e4d3663d6cdf02392bde6871dfaa047c",
          "message": "Feature/vertex subgraph (#1025)\n\n* implement vertex-induced subgraph\r\n\r\n* add vertex subgraph to api",
          "timestamp": "2023-06-07T15:17:51+01:00",
          "tree_id": "01259f4822e6ebdc888eff67c6e645c3316be509",
          "url": "https://github.com/Pometry/Raphtory/commit/07354ba9e4d3663d6cdf02392bde6871dfaa047c"
        },
        "date": 1686147779313,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 371055,
            "range": "± 6376",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1061091,
            "range": "± 19036",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1150792,
            "range": "± 23946",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 923889,
            "range": "± 222916",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2560830,
            "range": "± 297680",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a38c13147e3cb7c936bcf0c8e31605e0a86e5dab",
          "message": "make IntoDynamic public (#1029)",
          "timestamp": "2023-06-07T17:23:26+01:00",
          "tree_id": "149df45d25e76173b345e63f040cec5a99617d1a",
          "url": "https://github.com/Pometry/Raphtory/commit/a38c13147e3cb7c936bcf0c8e31605e0a86e5dab"
        },
        "date": 1686157074335,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 377459,
            "range": "± 27287",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1110967,
            "range": "± 42026",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1177471,
            "range": "± 52125",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1113219,
            "range": "± 254009",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 3086592,
            "range": "± 308471",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c60ef7bcf48469ec19a1bb2b40871970085c5414",
          "message": "Release v0.4.0 (#1030)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-06-07T18:15:00+01:00",
          "tree_id": "b538dfff87f945dfdaed052e96f45d7a12b192cd",
          "url": "https://github.com/Pometry/Raphtory/commit/c60ef7bcf48469ec19a1bb2b40871970085c5414"
        },
        "date": 1686160951750,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 295186,
            "range": "± 532",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 846203,
            "range": "± 3676",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 888141,
            "range": "± 7479",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 617784,
            "range": "± 106388",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2017839,
            "range": "± 148773",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3d5bd1c8722ededc0ca8a6f200a3b200e8d13af5",
          "message": "Update _release_python.yml (#1031)\n\nmissing interpreter",
          "timestamp": "2023-06-07T19:57:19+01:00",
          "tree_id": "cec723bb5ef4db9065a555f822cf2f80f0ed3490",
          "url": "https://github.com/Pometry/Raphtory/commit/3d5bd1c8722ededc0ca8a6f200a3b200e8d13af5"
        },
        "date": 1686164342417,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 327208,
            "range": "± 2571",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 905468,
            "range": "± 5851",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 941384,
            "range": "± 1964",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 435263,
            "range": "± 38247",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1814513,
            "range": "± 52987",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f04f322c46f9e3836daf4faff12f69b149bb77bc",
          "message": "Equivalence edge comparison and vertex comparison (#1035)\n\n* added rich comparison feature for equals and not equals and set hashing\r\n\r\n* add edge comparison and hashing",
          "timestamp": "2023-06-09T15:38:33+01:00",
          "tree_id": "70ab53dcd40e1adac23d11caa6c042df85f0f70b",
          "url": "https://github.com/Pometry/Raphtory/commit/f04f322c46f9e3836daf4faff12f69b149bb77bc"
        },
        "date": 1686321751119,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 342617,
            "range": "± 2308",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 936065,
            "range": "± 3199",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 966084,
            "range": "± 2666",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 443429,
            "range": "± 74744",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1758177,
            "range": "± 53267",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0daed912e2bb1a98bcaa741f44b2320539919939",
          "message": "Added basic filtering for nodes in graphql (#1037)\n\nAdded basic filtering for nodes",
          "timestamp": "2023-06-09T15:59:06+01:00",
          "tree_id": "d0cb3d2e7f641dcf20ff17d9fe8521277d15099b",
          "url": "https://github.com/Pometry/Raphtory/commit/0daed912e2bb1a98bcaa741f44b2320539919939"
        },
        "date": 1686323028785,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 339335,
            "range": "± 22623",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 957586,
            "range": "± 41608",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1078326,
            "range": "± 56189",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 969589,
            "range": "± 198006",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2474555,
            "range": "± 430673",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0daed912e2bb1a98bcaa741f44b2320539919939",
          "message": "Added basic filtering for nodes in graphql (#1037)\n\nAdded basic filtering for nodes",
          "timestamp": "2023-06-09T15:59:06+01:00",
          "tree_id": "d0cb3d2e7f641dcf20ff17d9fe8521277d15099b",
          "url": "https://github.com/Pometry/Raphtory/commit/0daed912e2bb1a98bcaa741f44b2320539919939"
        },
        "date": 1686328650319,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 371410,
            "range": "± 24924",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1049799,
            "range": "± 30209",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1084078,
            "range": "± 7390",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 773902,
            "range": "± 104466",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2519536,
            "range": "± 139690",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+iamsmkr@users.noreply.github.com",
            "name": "Shivam Kapoor",
            "username": "iamsmkr"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8d909ef1567209727c633dac9775952f1a6e923c",
          "message": "impl graph properties (#1041)\n\n* impl graph properties\r\n\r\n* add missing impl, impl pytests\r\n\r\n* fix\r\n\r\n* add docs, comments",
          "timestamp": "2023-06-13T16:28:03+01:00",
          "tree_id": "b55a28fa8d253d760d0d2279f5c6ada97ef0b225",
          "url": "https://github.com/Pometry/Raphtory/commit/8d909ef1567209727c633dac9775952f1a6e923c"
        },
        "date": 1686670332199,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 322661,
            "range": "± 1832",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 916036,
            "range": "± 4681",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 952364,
            "range": "± 4071",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 700347,
            "range": "± 150425",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2324390,
            "range": "± 180789",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+iamsmkr@users.noreply.github.com",
            "name": "Shivam Kapoor",
            "username": "iamsmkr"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "5c6c904e2d9e41cb58ce0345d1eecdc32c8fa700",
          "message": "Features/materialize subgraph (#1044)\n\n* add vertex static props\r\n\r\n* impl graph materialize and edge static properties api\r\n\r\n* add doc comments\r\n\r\n* fix materialize api\r\n\r\n* add static properties apis to python, fix default layer issue with materialized graph, impl materialized graph tests\r\n\r\n* impl static properties on graph\r\n\r\n* add graph static properties to materialized static properties\r\n\r\n* add doc comments on py-raphtory\r\n\r\n* add doc comments to raphtory",
          "timestamp": "2023-06-15T11:03:37+01:00",
          "tree_id": "82909d988f43b25ef8bb5abadaa7dddaf510acaa",
          "url": "https://github.com/Pometry/Raphtory/commit/5c6c904e2d9e41cb58ce0345d1eecdc32c8fa700"
        },
        "date": 1686823713892,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 370911,
            "range": "± 11102",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1104170,
            "range": "± 43630",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1186786,
            "range": "± 42621",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1217782,
            "range": "± 182668",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2695351,
            "range": "± 267613",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33124479+narnolddd@users.noreply.github.com",
            "name": "Naomi Arnold",
            "username": "narnolddd"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f72d2d95ca5ab72e3a894ce4c4b46dae3788533b",
          "message": "Stack exchange data + fixed motifs (#1039)\n\n* impl edges gql\r\n\r\n* company house data\r\n\r\n* add properties and project wide fmt\r\n\r\n* impl neoghbours\r\n\r\n* adding flag\r\n\r\n* add inneighs\r\n\r\n* stack exchange example nb\r\n\r\n* adding plotting utils\r\n\r\n* fix null models and start stackexchange example for notebook\r\n\r\n* plot dump\r\n\r\n* update nb\r\n\r\n* got 3 node motif algo back to working version\r\n\r\n* motifs algorithm works!!!!\r\n\r\n* Correct motif mapper\r\n\r\n* neaten up notebook\r\n\r\n* remove results and python cache file\r\n\r\n* motif rename to be more specific\r\n\r\n* remove accidentally added results file\r\n\r\n* three node eval wip\r\n\r\n* fix broken refactor\r\n\r\n* new function name\r\n\r\n---------\r\n\r\nCo-authored-by: Shivam Kapoor <4599890+iamsmkr@users.noreply.github.com>\r\nCo-authored-by: RachelChan <25484244+rachchan@users.noreply.github.com>\r\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2023-06-15T11:13:51+01:00",
          "tree_id": "335de30ff9d287a1c394602615f8e7022486d707",
          "url": "https://github.com/Pometry/Raphtory/commit/f72d2d95ca5ab72e3a894ce4c4b46dae3788533b"
        },
        "date": 1686824265896,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 329251,
            "range": "± 2613",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 922299,
            "range": "± 1375",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 956015,
            "range": "± 2840",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 460382,
            "range": "± 64914",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1824441,
            "range": "± 69913",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+iamsmkr@users.noreply.github.com",
            "name": "Shivam Kapoor",
            "username": "iamsmkr"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b2f157edcc10b67b36d7cdced9e5d8cf9864e6f2",
          "message": "fix loader (#1048)",
          "timestamp": "2023-06-16T11:18:11+01:00",
          "tree_id": "f5a2b275389b85045fd7d66c45bb0a8b4adcaa6f",
          "url": "https://github.com/Pometry/Raphtory/commit/b2f157edcc10b67b36d7cdced9e5d8cf9864e6f2"
        },
        "date": 1686910947742,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 332685,
            "range": "± 2388",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 925235,
            "range": "± 3340",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 960364,
            "range": "± 5058",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 516011,
            "range": "± 70123",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1915844,
            "range": "± 89442",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d2b1bc813932b16428736551a6c66821c0550ab3",
          "message": "Feature/edge deletions (#1049)\n\n* add deletion timestamps to EdgeLayer\r\n\r\n* work in progress on getting traits back in order\r\n\r\n* it compiles but neighbours for window seems broken?\r\n\r\n* fix windowing bugs\r\n\r\n* Minor refactor to bring back GraphViewInternalOps as a catch-all trait for box-able graphs\r\n\r\n* fix  the dynamic boxed graph\r\n\r\n* fix broken tests that were checking for buggy behaviour\r\n\r\n* fix issues after merge\r\n\r\n* clean up a lot of warnings\r\n\r\n* more warnings gone\r\n\r\n* start persistent edge view\r\n\r\n* address review comments",
          "timestamp": "2023-06-16T17:30:18+01:00",
          "tree_id": "da12f46ebac2bab551c5f594bdca15b1974bce43",
          "url": "https://github.com/Pometry/Raphtory/commit/d2b1bc813932b16428736551a6c66821c0550ab3"
        },
        "date": 1686933113660,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 337452,
            "range": "± 2717",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 934874,
            "range": "± 2096",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 970646,
            "range": "± 2220",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 529442,
            "range": "± 94251",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1880787,
            "range": "± 71794",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33124479+narnolddd@users.noreply.github.com",
            "name": "Naomi Arnold",
            "username": "narnolddd"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8cf65b4469a7311e968cbc0f3042aa5e1c7579ba",
          "message": "Algo docs (#1047)\n\n* some minor changes to section titles\r\n\r\n* algorithm docs\r\n\r\n* oops forgot graph density\r\n\r\n* change generic taint to temporal reachability\r\n\r\n* changed python references to generic taint\r\n\r\n* forgot to check examples build...\r\n\r\n* further examples\r\n\r\n* realised that Some of an empty vector is None\r\n\r\n* added python docstrings for null models\r\n\r\n* misesed python test",
          "timestamp": "2023-06-19T10:52:29+01:00",
          "tree_id": "822a46c2804fd71fd7b371b79fd5fa60a20ed0e1",
          "url": "https://github.com/Pometry/Raphtory/commit/8cf65b4469a7311e968cbc0f3042aa5e1c7579ba"
        },
        "date": 1687168680633,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 376321,
            "range": "± 11580",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1145880,
            "range": "± 74002",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1218946,
            "range": "± 55628",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1193701,
            "range": "± 305456",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 3051838,
            "range": "± 423707",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2f68fb52d8c840ff01133ee4efff54e3ee1db092",
          "message": "remove commented out tests for apis that no longer exist (#1053)",
          "timestamp": "2023-06-19T10:59:04+01:00",
          "tree_id": "9c1a29306b6102bf3da6a8071a3911b09d341db1",
          "url": "https://github.com/Pometry/Raphtory/commit/2f68fb52d8c840ff01133ee4efff54e3ee1db092"
        },
        "date": 1687169090139,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 386824,
            "range": "± 13414",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1181542,
            "range": "± 30910",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1214960,
            "range": "± 24825",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1326393,
            "range": "± 233353",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 3059953,
            "range": "± 347299",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "louisch@fastmail.com",
            "name": "Louis Chan",
            "username": "louisch"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e40d99104209f14ceb1bbbbf13d72007e328db5e",
          "message": "Add new filter for filtering properties in GraphQL (#1052)",
          "timestamp": "2023-06-19T11:06:41+01:00",
          "tree_id": "a31658b18ba63b04fad6084294b3defc727216eb",
          "url": "https://github.com/Pometry/Raphtory/commit/e40d99104209f14ceb1bbbbf13d72007e328db5e"
        },
        "date": 1687169469863,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 343299,
            "range": "± 1491",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 928350,
            "range": "± 1630",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 960595,
            "range": "± 5009",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 513775,
            "range": "± 59843",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1956149,
            "range": "± 102552",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "92f83bb0e1222f2096f6f64c40002b081927214b",
          "message": "Adding basic layer filtering for graphql functions (#1043)\n\n* Add layer to edge\r\n\r\n* GraphQL Node changes\r\n\r\n* Move dynamic graph to core/db and add layer filtering for graphql\r\n\r\n* Had to duplicate dynamic\r\n\r\n* Enable correct layer semantics\r\n\r\n* Added subgraph back to python dynamic graph\r\n\r\n* Fixed merge",
          "timestamp": "2023-06-19T13:54:18+01:00",
          "tree_id": "b558c505e5af2092cbc2e829f191d49ee4fa0436",
          "url": "https://github.com/Pometry/Raphtory/commit/92f83bb0e1222f2096f6f64c40002b081927214b"
        },
        "date": 1687179636712,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 343014,
            "range": "± 2894",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 938177,
            "range": "± 19542",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 966351,
            "range": "± 5447",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 602421,
            "range": "± 63192",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2026727,
            "range": "± 98633",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a57e19d96f5d656469a973d5e776f6bbcc3dc189",
          "message": "panic when errors are found when loading graphs (#1056)",
          "timestamp": "2023-06-19T14:37:34+01:00",
          "tree_id": "47e8ba4ac0e455a6433a620f8993820ae43eb381",
          "url": "https://github.com/Pometry/Raphtory/commit/a57e19d96f5d656469a973d5e776f6bbcc3dc189"
        },
        "date": 1687182099253,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 342589,
            "range": "± 1140",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 930462,
            "range": "± 1970",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 961551,
            "range": "± 2788",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 475583,
            "range": "± 53500",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1843073,
            "range": "± 65156",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+iamsmkr@users.noreply.github.com",
            "name": "Shivam Kapoor",
            "username": "iamsmkr"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8f36d3b3dc24de081a7684c66159d4a1286f1a84",
          "message": "impl subgraphs (#1057)\n\n* impl subgraphs\r\n\r\n* panic when graph already found",
          "timestamp": "2023-06-19T17:41:57+01:00",
          "tree_id": "fc3400b612c7851872f1cdd82d6a3eb1624420c0",
          "url": "https://github.com/Pometry/Raphtory/commit/8f36d3b3dc24de081a7684c66159d4a1286f1a84"
        },
        "date": 1687193213945,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 364597,
            "range": "± 26291",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1084623,
            "range": "± 24859",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1112790,
            "range": "± 40169",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1182506,
            "range": "± 206324",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2492394,
            "range": "± 352462",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ae08e5386536f3d72557194e576589a32e142c82",
          "message": "Remove dynamic graph (#1055)\n\n* No need for a special DynamicGraph struct anymore\r\n\r\n* no need for DynamicGraph struct\r\n\r\n* refactor to make python bindings and io features rather than crates\r\n\r\n* make sure we still run all the tests\r\n\r\n* remove left-over files\r\n\r\n* fix test workflow?\r\n\r\n* remove deleted packages from release workflow\r\n\r\n* add python and io to code coverage",
          "timestamp": "2023-06-21T00:28:46+01:00",
          "tree_id": "ac3bcee96d6578c1805bf96112abea2fbc3695ba",
          "url": "https://github.com/Pometry/Raphtory/commit/ae08e5386536f3d72557194e576589a32e142c82"
        },
        "date": 1687304047940,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 362747,
            "range": "± 716",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1050661,
            "range": "± 2823",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1097239,
            "range": "± 3394",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 813074,
            "range": "± 109260",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2579723,
            "range": "± 177449",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ff501016aa9aa677288b677013770cc4d474ffc4",
          "message": "Update binder_auto_build.yml (#1059)",
          "timestamp": "2023-06-21T11:39:23+01:00",
          "tree_id": "2fc449f45099dfa43fde5e24f4a9427511e22b3f",
          "url": "https://github.com/Pometry/Raphtory/commit/ff501016aa9aa677288b677013770cc4d474ffc4"
        },
        "date": 1687344244694,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 338077,
            "range": "± 2242",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 953717,
            "range": "± 2910",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 986739,
            "range": "± 2140",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 564940,
            "range": "± 100840",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1958740,
            "range": "± 94138",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8a67f3c4d2bf92047e31aa51ae2a1bbcda1123e1",
          "message": "Release v0.4.1 (#1060)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-06-21T12:54:41+01:00",
          "tree_id": "32a9b12814e7ee94f3db365c521c39028b0888c0",
          "url": "https://github.com/Pometry/Raphtory/commit/8a67f3c4d2bf92047e31aa51ae2a1bbcda1123e1"
        },
        "date": 1687348754616,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 341629,
            "range": "± 2360",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 923983,
            "range": "± 1496",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 955001,
            "range": "± 2636",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 543295,
            "range": "± 95054",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1887246,
            "range": "± 83640",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "29a0bf8adae8743c619f603b043c93058e8e6c2e",
          "message": "Bug/partial windows (#1063)\n\n* Change semantics of `expanding` and `rolling` to include partial window at end\r\n\r\n* fix tests\r\n\r\n* improve docs for time windows\r\n\r\n* update python tests to reflect partial windows at end",
          "timestamp": "2023-06-22T12:07:27+02:00",
          "tree_id": "58fcb0b0cad7ff3f87c0971f843a3cc15d42e081",
          "url": "https://github.com/Pometry/Raphtory/commit/29a0bf8adae8743c619f603b043c93058e8e6c2e"
        },
        "date": 1687428764035,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 393485,
            "range": "± 1613",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1090711,
            "range": "± 11918",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1141329,
            "range": "± 6859",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 819029,
            "range": "± 109253",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2782649,
            "range": "± 163675",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4f53db429f08f453f9c7121ba2715e3de09b0768",
          "message": "Feature/graph with deletions (#1065)\n\n* Change time semantics for subgraph and add more tests. Note this makes vertex history behaviour a bit strange for subgraph as it includes timestamps for edges that are outside of the subgraph but there is no easy way to fix this without storing more info during ingestion.\r\n\r\n* start tweaking edge time semantics for exploded edges\r\n\r\n* think about making traits for mutation ops\r\n\r\n* start fleshing out api\r\n\r\n* implement mutations API\r\n\r\n* fix properties and make everything compile again\r\n\r\n* fix python properties conversions so we don't end up with 32bit ints and floats\r\n\r\n* clean up docstrings\r\n\r\n* simplify python wrappers\r\n\r\n* fix add vertex with custom time\r\n\r\n* make inheritance of graph ops easier to use\r\n\r\n* more cleanup and fix js\r\n\r\n* add history methods for deletions\r\n\r\n* fix broken import\r\n\r\n* fix time semantics for exploded edges with deletions and start figuring out materialize with deletions\r\n\r\n* trait cleanup and make materialize work for deletions\r\n\r\n* mayor clean up of imports and fix python\r\n\r\n* more python cleanup\r\n\r\n* fix and cleanup imports\r\n\r\n* clean up python wrappers by implementing pyo3 conversion traits\r\n\r\n* consistent name\r\n\r\n* add python graph with deletions\r\n\r\n* expose GraphWithDeletions to python\r\n\r\n* fix materialize for window with deletions\r\n\r\n* restore old argument name\r\n\r\n* fix exploded latest time for deletions\r\n\r\n* delete broken test\r\n\r\n* Rename Inheritable to Base\r\n\r\n* Avoid hitting SNAP for tests\r\n\r\n* clean up warnings",
          "timestamp": "2023-06-26T13:59:21+02:00",
          "tree_id": "0630b69edf48c6195307a67d461b578538780d57",
          "url": "https://github.com/Pometry/Raphtory/commit/4f53db429f08f453f9c7121ba2715e3de09b0768"
        },
        "date": 1687781136692,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 650334,
            "range": "± 30274",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1268314,
            "range": "± 136983",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1344217,
            "range": "± 82940",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1220868,
            "range": "± 219045",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2310350,
            "range": "± 340355",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1ea7e096e0adbcdc19097647600a5e1febd0a24a",
          "message": "add a python test for layer operations (#1068)",
          "timestamp": "2023-06-26T14:46:59+01:00",
          "tree_id": "80008d8ee6c6af990f28043e58e534511e916401",
          "url": "https://github.com/Pometry/Raphtory/commit/1ea7e096e0adbcdc19097647600a5e1febd0a24a"
        },
        "date": 1687787498646,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 536188,
            "range": "± 4259",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 978295,
            "range": "± 3646",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1020258,
            "range": "± 5466",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 914234,
            "range": "± 145116",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1803621,
            "range": "± 206213",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d5837e95e9b085a1dcb0011170c48dd9361f2156",
          "message": "bump neo4rs version (#1069)\n\nbump neo4rs version since 0.6.0 seems to not actually have `default` for config?",
          "timestamp": "2023-06-26T16:09:58+01:00",
          "tree_id": "e4f202da6ef829815e52c9a7bf2117b2e3e015bb",
          "url": "https://github.com/Pometry/Raphtory/commit/d5837e95e9b085a1dcb0011170c48dd9361f2156"
        },
        "date": 1687792510779,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 644990,
            "range": "± 6589",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1146546,
            "range": "± 14260",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1209378,
            "range": "± 14110",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 962249,
            "range": "± 123399",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2024128,
            "range": "± 188769",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b17b4bd6fe085d64e6189a198875b66d229653dc",
          "message": "Dynamic graph (#1076)\n\n* Bring back the DynamicGraph newtype struct such that it is possible to implement IntoPy for it\r\n\r\n* make CsvErr python friendly",
          "timestamp": "2023-06-28T13:34:01+01:00",
          "tree_id": "c8beafe300c6f36b9edcea4f3f2f3226f80e89c0",
          "url": "https://github.com/Pometry/Raphtory/commit/b17b4bd6fe085d64e6189a198875b66d229653dc"
        },
        "date": 1687955940867,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 728355,
            "range": "± 2797",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1298287,
            "range": "± 11137",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1341577,
            "range": "± 6067",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 950827,
            "range": "± 72138",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1812987,
            "range": "± 68313",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e05480ebbda8e95df560a9269d9388b487eae99d",
          "message": "Fix/master (#1077)\n\n* Fixed Graph still taking shard count\r\n\r\n* Formatted full project\r\n\r\n* One more",
          "timestamp": "2023-06-28T14:20:01+01:00",
          "tree_id": "f85f66c12d504cc88ece59f44d6554a85de6606f",
          "url": "https://github.com/Pometry/Raphtory/commit/e05480ebbda8e95df560a9269d9388b487eae99d"
        },
        "date": 1687958515209,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 678471,
            "range": "± 27234",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1201268,
            "range": "± 3307",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1229167,
            "range": "± 15626",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1068471,
            "range": "± 148801",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1914900,
            "range": "± 58614",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "791788d46fefc43211f06aff0234c378d050b235",
          "message": "Release v0.4.2 (#1078)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-06-28T14:30:12+01:00",
          "tree_id": "5357861891c23ea75bd98aa7dd509afd737327ab",
          "url": "https://github.com/Pometry/Raphtory/commit/791788d46fefc43211f06aff0234c378d050b235"
        },
        "date": 1687959134848,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 721998,
            "range": "± 1500",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1297901,
            "range": "± 9502",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1329986,
            "range": "± 3255",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 828433,
            "range": "± 43223",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1733938,
            "range": "± 50436",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4fa687fc02abd90c887cf15bfc92a0c0cc164cd0",
          "message": "Fix state post remove shards (#1080)\n\n* chunk the state correctly and iterate over it in the right way\r\n\r\n* fix state issues with the latest shard free approach\r\n\r\n* undo fn zero() for VecArray\r\n\r\n* remove shard from sx_superuser_graph\r\n\r\n* fix python test",
          "timestamp": "2023-06-29T15:16:31+01:00",
          "tree_id": "292ad517b28717b2f923c6fe1f2c3cc8a40e61ec",
          "url": "https://github.com/Pometry/Raphtory/commit/4fa687fc02abd90c887cf15bfc92a0c0cc164cd0"
        },
        "date": 1688048533029,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 704813,
            "range": "± 40454",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1416956,
            "range": "± 75778",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1521976,
            "range": "± 98119",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1166090,
            "range": "± 178953",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2231878,
            "range": "± 167058",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c968832add562e93167e90a8e6390da1d4c2e636",
          "message": "Graph properties (#1079)\n\n* impl graph static properties graphql api\r\n\r\n* add more filters\r\n\r\n* add multiple layers filter to degree\r\n\r\n* add property_history to node\r\n\r\n* read files instead of directories from graphql server\r\n\r\n---------\r\n\r\nCo-authored-by: Shivam Kapoor <4599890+iamsmkr@users.noreply.github.com>",
          "timestamp": "2023-06-29T16:47:45+01:00",
          "tree_id": "e6e8663364b5fb8a7d1c708a79a27e6722e2afaf",
          "url": "https://github.com/Pometry/Raphtory/commit/c968832add562e93167e90a8e6390da1d4c2e636"
        },
        "date": 1688053952198,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 745776,
            "range": "± 1254",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1311994,
            "range": "± 2452",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1348945,
            "range": "± 4798",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 994780,
            "range": "± 75186",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1848298,
            "range": "± 76584",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2363e8fb1c4bb059a474fb8be339b4412214bf84",
          "message": "stop automatically reading hidden files (#1083)",
          "timestamp": "2023-06-30T15:13:36+01:00",
          "tree_id": "a97b1a0d5de54bca4d76fc2286b94910bc4416dd",
          "url": "https://github.com/Pometry/Raphtory/commit/2363e8fb1c4bb059a474fb8be339b4412214bf84"
        },
        "date": 1688134741688,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 821698,
            "range": "± 2727",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1476506,
            "range": "± 16108",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1507078,
            "range": "± 2941",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1158491,
            "range": "± 107577",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2182526,
            "range": "± 98132",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "44354eae7a97c8ddf4c6041e9ee946ffce2748ec",
          "message": "Fixed all warnings and refactor module structure (#1081)\n\n* Fixed all warnings\r\n\r\n* first attempt at core refactor\r\n\r\n* Refactored db\r\n\r\n* Fixed all tests in benchmark graphql python and core\r\n\r\n* Added rustfmt file\r\n\r\n* fix neo tests\r\n\r\n* Renamed views\r\n\r\n* more tests\r\n\r\n* more tests\r\n\r\n* Fix JS\r\n\r\n* more tests\r\n\r\n* Fix all imports\r\n\r\n* Moved comparison benchmarks\r\n\r\n* Changed outer tgraph to entities\r\n\r\n* refactored python\r\n\r\n* Removed Perspectives as not being used\r\n\r\n* remove perspective ref\r\n\r\n* Final fmt",
          "timestamp": "2023-07-03T20:21:10+01:00",
          "tree_id": "77897c90092c3b96beef70c8ed52aa906bf245fe",
          "url": "https://github.com/Pometry/Raphtory/commit/44354eae7a97c8ddf4c6041e9ee946ffce2748ec"
        },
        "date": 1688412392012,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 783370,
            "range": "± 10492",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1433754,
            "range": "± 4174",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1466612,
            "range": "± 3457",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1158618,
            "range": "± 87277",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2220975,
            "range": "± 99255",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fdf064c84b369c1f38a2f7f45216f2bdf9b57b6f",
          "message": "index add_vertex and search by name (#1084)\n\nBasic search functionality for raphtory Graph",
          "timestamp": "2023-07-05T12:04:57+01:00",
          "tree_id": "35a2016d779ab04d1e59c0dea3f376c9de8bcb23",
          "url": "https://github.com/Pometry/Raphtory/commit/fdf064c84b369c1f38a2f7f45216f2bdf9b57b6f"
        },
        "date": 1688555467503,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 697774,
            "range": "± 2492",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1233052,
            "range": "± 3449",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1261983,
            "range": "± 3543",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1074699,
            "range": "± 110396",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2020965,
            "range": "± 108334",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "03d68d2a66c3910df685c38174a931c80ed012f1",
          "message": "Clean docs (#1088)\n\n* - [x] Added a straight redirect if anyone clicks on the rust docs link it goes into cargo right away\r\n- [x] Add a screenshot of the crate page into the rust\r\n- [x] Removed the roadmap section and add an issue so we can add it back in\r\n- [x] Removed the user guide section\r\n- [x] Removed intro to raphtory and tutorials\r\n- [x] Used the summary from the paper in the first page of the docs and linked to the paper\r\n- [x] reduced font size of tutoiral questions\r\n    - [x] Expand description in visualising the graph\r\n- [x] Moved license to a link\r\n- [x] fixed getting started typos\r\n- [x] Slack link does now works\r\n\r\n* - [x] Rewrite the text to be more developer friendly\r\n- [x] Use the overview from the paper into the getting started questions and its a summary of raphtory is used for, with he overview + projects that use raphtory and the 3 functionalities\r\n- [x] Package overview is a summary of raphtory\r\n- [x] In overview replace the text with the sumarry from the paper\r\n- [x] Move project overview into getting started\r\n- [x] \t\tLink the getting started tutorials in tutorials\r\n- [x] Moved far to separate section and reduced header sizes\r\n- [x] Fixed the broken text copied in source\r\n\r\n* add autosummary toc to raphtory python docs\r\n\r\n* update sphinx version, add missing rst files\r\n\r\n* deps?\r\n\r\n* filled in all the missing docs for all the missing public functions\r\n\r\n* fix environment for rtd\r\n\r\n* update python version\r\n\r\n* bad abi\r\n\r\n* bad ncurses\r\n\r\n* all more than\r\n\r\n* shivam did not like the color, so we added a darker one",
          "timestamp": "2023-07-06T09:38:43+01:00",
          "tree_id": "f9e49ccb1e9d742a98ff119376a99af316e15b53",
          "url": "https://github.com/Pometry/Raphtory/commit/03d68d2a66c3910df685c38174a931c80ed012f1"
        },
        "date": 1688633158318,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 807829,
            "range": "± 9318",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1487324,
            "range": "± 37773",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1515573,
            "range": "± 10020",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1216980,
            "range": "± 78633",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2388082,
            "range": "± 96022",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "431b2cc86e6a733a3db0cba5bd3e433fb91d9fc1",
          "message": "Reduce tests that run on python workflows during PRs (#1090)\n\n* checkin\r\n\r\n* restrict python versions in github workflows during PR to only test version 3.11, but allow both 3.9 and 3.11 to be tested upon push to master\r\n\r\n* should fix my if else\r\n\r\n* small test script\r\n\r\n* no amd64 for windows\r\n\r\n* setup all versions inside setup, and allow it to find them all\r\n\r\n* without qemu?\r\n\r\n* without qemu?\r\n\r\n* test 3.7 and 3.11\r\n\r\n* test 3.7 and 3.11\r\n\r\n* fix workflow\r\n\r\n* delete test workflow",
          "timestamp": "2023-07-06T15:58:58+01:00",
          "tree_id": "72ae2d2b01c4d9db79d93ba8bb992758d705d7b9",
          "url": "https://github.com/Pometry/Raphtory/commit/431b2cc86e6a733a3db0cba5bd3e433fb91d9fc1"
        },
        "date": 1688655900520,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 741361,
            "range": "± 2162",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1319239,
            "range": "± 2972",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1357142,
            "range": "± 1885",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 854051,
            "range": "± 49645",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1780012,
            "range": "± 79682",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3832ebf498dfce595e497293590c91cd92e23d57",
          "message": "Search edges (#1091)\n\nFix issue that was reloading the graph on every graphql query\r\nResolve the duplication issue by changing way we index vertices and adding multiple values to the same document\r\nAdd indexing for edges",
          "timestamp": "2023-07-06T17:14:52+01:00",
          "tree_id": "233eca9e068978bcf97e7c277d45a1d100c00893",
          "url": "https://github.com/Pometry/Raphtory/commit/3832ebf498dfce595e497293590c91cd92e23d57"
        },
        "date": 1688660450937,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 756353,
            "range": "± 3468",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1327023,
            "range": "± 3212",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1363711,
            "range": "± 3230",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 908748,
            "range": "± 60453",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1754449,
            "range": "± 55789",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "087a5f050d398ffc67085958af2c7a9d81bd976b",
          "message": "Release v0.4.3 (#1093)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-07-06T19:59:31+01:00",
          "tree_id": "1e1ea12013cff1a747940e5c37191df13ca993fa",
          "url": "https://github.com/Pometry/Raphtory/commit/087a5f050d398ffc67085958af2c7a9d81bd976b"
        },
        "date": 1688670399169,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 815553,
            "range": "± 3945",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1476176,
            "range": "± 1991",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1516131,
            "range": "± 4867",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1252594,
            "range": "± 58326",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2296522,
            "range": "± 90471",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "20f05179c1be21044e152087521aaeff5c919206",
          "message": "fix benchmark download to point to osf instead of stinky git lfs (#1096)",
          "timestamp": "2023-07-07T12:57:22+01:00",
          "tree_id": "b8fa4b9b61986eaf6da3c2ae37f458a1e252293f",
          "url": "https://github.com/Pometry/Raphtory/commit/20f05179c1be21044e152087521aaeff5c919206"
        },
        "date": 1688731497310,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 805881,
            "range": "± 18519",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1486372,
            "range": "± 86585",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1529212,
            "range": "± 42377",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1461122,
            "range": "± 194626",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2547854,
            "range": "± 266321",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "7692a8497f1624e646fc31ce980595f365668426",
          "message": "bug with not installed docker via pip causing drivers to not instatiate  (#1097)\n\n* fix benchmark download to point to osf instead of stinky git lfs\r\n\r\n* fix broken docker",
          "timestamp": "2023-07-07T14:49:43+01:00",
          "tree_id": "ef99e4b35a2c7d435b80d103154017c6eb78b951",
          "url": "https://github.com/Pometry/Raphtory/commit/7692a8497f1624e646fc31ce980595f365668426"
        },
        "date": 1688738137873,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 675361,
            "range": "± 2583",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1222725,
            "range": "± 2473",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1255578,
            "range": "± 6196",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1010147,
            "range": "± 91485",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1943343,
            "range": "± 100027",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f64e5d1db9f920df24a9f178c32bd6b470c54171",
          "message": "Add faster failure, move to python folder, fix for 0.4.3 (#1098)\n\n* fail faster\r\n\r\n* 0.4.3 broke the benchmark",
          "timestamp": "2023-07-07T15:17:02+01:00",
          "tree_id": "b7267b545d2ba35eb0ebbbb3c541ab74254dcd5b",
          "url": "https://github.com/Pometry/Raphtory/commit/f64e5d1db9f920df24a9f178c32bd6b470c54171"
        },
        "date": 1688739769294,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 762421,
            "range": "± 3794",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1343208,
            "range": "± 3669",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1379992,
            "range": "± 3256",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 957759,
            "range": "± 67606",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1824489,
            "range": "± 76475",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3de7f6aab4db5eda886bcb46f66a62eb3667d3bf",
          "message": "Update readme.md",
          "timestamp": "2023-07-07T20:30:20+01:00",
          "tree_id": "f526e7fc1b8e41e6bb22266964f646cb331aa631",
          "url": "https://github.com/Pometry/Raphtory/commit/3de7f6aab4db5eda886bcb46f66a62eb3667d3bf"
        },
        "date": 1688758620636,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 676918,
            "range": "± 2965",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1226253,
            "range": "± 53169",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1261565,
            "range": "± 6570",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1089709,
            "range": "± 183957",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1931178,
            "range": "± 162543",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "32cec73c84bbfa7f5773d76d139bd4f7212be7a5",
          "message": "Rusty apis (#1101)\n\n* improve slightly the rust prop api\r\n\r\n* add the slight improvement to add_edge\r\n\r\n* remove trait Properties\r\n\r\n* fix issue in benchmarks\r\n\r\n* refactor the trait by bringing Properties back",
          "timestamp": "2023-07-10T17:06:31+01:00",
          "tree_id": "8842af9492dc1563d8288088ede215cf9951c9ec",
          "url": "https://github.com/Pometry/Raphtory/commit/32cec73c84bbfa7f5773d76d139bd4f7212be7a5"
        },
        "date": 1689005651645,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 809494,
            "range": "± 4164",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1467470,
            "range": "± 5674",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1506675,
            "range": "± 3490",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1291409,
            "range": "± 78287",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2359444,
            "range": "± 73216",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33124479+narnolddd@users.noreply.github.com",
            "name": "Naomi Arnold",
            "username": "narnolddd"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "592e22535b3e4dc8b51eb20b1235ece7ede33b09",
          "message": "3nm docs (#1108)\n\n* privatise some functions and initial docstring for three node motifs\r\n\r\n* adding rust docs for temporal motifs\r\n\r\n* added python docs",
          "timestamp": "2023-07-13T11:03:46+01:00",
          "tree_id": "9529476f7d6057395bc575447051fdf39d9ddfeb",
          "url": "https://github.com/Pometry/Raphtory/commit/592e22535b3e4dc8b51eb20b1235ece7ede33b09"
        },
        "date": 1689243063756,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 713036,
            "range": "± 46957",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1289072,
            "range": "± 71304",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1346977,
            "range": "± 39371",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1120095,
            "range": "± 157711",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2293571,
            "range": "± 375392",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "0227ea6b7a465df2731bca6ffb1f9ab2c7ab07b9",
          "message": "Property returns value (#1106)\n\nchange graphql property function to return values",
          "timestamp": "2023-07-13T14:16:58+02:00",
          "tree_id": "35e1b6c9777dffba24518e338db09595ad9d66b1",
          "url": "https://github.com/Pometry/Raphtory/commit/0227ea6b7a465df2731bca6ffb1f9ab2c7ab07b9"
        },
        "date": 1689250992874,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 669998,
            "range": "± 5039",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1222644,
            "range": "± 10659",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1242296,
            "range": "± 6412",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1019634,
            "range": "± 94998",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1882610,
            "range": "± 81695",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "45623dd628975cecbc62be537e133876a6ab31e1",
          "message": "update the python loader to include polars dataframe (#1104)\n\n* update the python loader to include polars dataframe\r\n\r\n* add test to load from polars\r\n\r\n* load property works\r\n\r\n* add props for string vertex type too\r\n\r\n* remove target from cache\r\n\r\n* temporary remove comparison benchmark\r\n\r\n* remove polars\r\n\r\n* remove polars and read edge list from pandas\r\n\r\n* fix CI build\r\n\r\n* add vertex load from pandas and 2 ways of loading\r\n\r\n* added tests for all loading functions\r\n\r\n* pulled the pandas loaders int pandas.rs\r\n\r\n* add rust test for loading from arrow arrays\r\n\r\n* improve testing",
          "timestamp": "2023-07-13T14:59:57+01:00",
          "tree_id": "e0378aaa08e38b5cd0ea15e47a5d657488a1262f",
          "url": "https://github.com/Pometry/Raphtory/commit/45623dd628975cecbc62be537e133876a6ab31e1"
        },
        "date": 1689257228387,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 802690,
            "range": "± 3627",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1443747,
            "range": "± 3025",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1473151,
            "range": "± 3552",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1188406,
            "range": "± 92639",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2138950,
            "range": "± 97729",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "77cfb5c604c3d3eb23e44b29d17e98b60a4369b4",
          "message": "remove the pandas dependency to 1.3.3 (#1111)",
          "timestamp": "2023-07-14T11:07:31+01:00",
          "tree_id": "e1ab098dc62420a196ee3c6779b3b1a36be6280e",
          "url": "https://github.com/Pometry/Raphtory/commit/77cfb5c604c3d3eb23e44b29d17e98b60a4369b4"
        },
        "date": 1689329623017,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 765241,
            "range": "± 2584",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1373493,
            "range": "± 3172",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1406164,
            "range": "± 3838",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1013247,
            "range": "± 74603",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1870029,
            "range": "± 87338",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c44c2dfcfeac048f5491a972efe395bf6c646a32",
          "message": "remove support for py3.7 (#1113)",
          "timestamp": "2023-07-14T12:49:24+01:00",
          "tree_id": "29702e64faed11efb5b74b808922ebe296a4f9da",
          "url": "https://github.com/Pometry/Raphtory/commit/c44c2dfcfeac048f5491a972efe395bf6c646a32"
        },
        "date": 1689335729151,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 693643,
            "range": "± 3584",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1239782,
            "range": "± 24512",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1272190,
            "range": "± 2108",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 998240,
            "range": "± 89103",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1866215,
            "range": "± 102766",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "6e7d1c7d43293226a7b4de9052187f5997b901e6",
          "message": "fix python 3.8 and 3.9 (#1114)",
          "timestamp": "2023-07-14T17:28:03+01:00",
          "tree_id": "c6c0972c062411c0ac21cb4a0ff68b1e4810dfdb",
          "url": "https://github.com/Pometry/Raphtory/commit/6e7d1c7d43293226a7b4de9052187f5997b901e6"
        },
        "date": 1689352503982,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 827671,
            "range": "± 20980",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1448785,
            "range": "± 43504",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1496396,
            "range": "± 50336",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1078277,
            "range": "± 130848",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2004038,
            "range": "± 102395",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c3c7f16d6d62e356f1c6055617485d98cb3aae89",
          "message": "fix subgraph neighbours returning duplicates (#1116)",
          "timestamp": "2023-07-17T14:37:13+02:00",
          "tree_id": "5fda7fe1e237d3c8402d08d87758feeb982f4b1c",
          "url": "https://github.com/Pometry/Raphtory/commit/c3c7f16d6d62e356f1c6055617485d98cb3aae89"
        },
        "date": 1689597805178,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 710107,
            "range": "± 3036",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1248736,
            "range": "± 2543",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1281358,
            "range": "± 3642",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1038404,
            "range": "± 95013",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1917154,
            "range": "± 93847",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "496a7fd4908205b2655feb71cb8401ce10ed905a",
          "message": "Bug/compute state (#1122)\n\n* add test for triangle count on subgraph\r\n\r\n* fix the issue with compute and subgraphs\r\n\r\n---------\r\n\r\nCo-authored-by: Lucas Jeub <lucas.jeub@pometry.com>",
          "timestamp": "2023-07-17T18:14:59+01:00",
          "tree_id": "48c5f4650e948a16a53e5686d0cdb250d12e0b94",
          "url": "https://github.com/Pometry/Raphtory/commit/496a7fd4908205b2655feb71cb8401ce10ed905a"
        },
        "date": 1689614495063,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 712329,
            "range": "± 40471",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1235800,
            "range": "± 55340",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1273265,
            "range": "± 30971",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 876623,
            "range": "± 80243",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1750795,
            "range": "± 85056",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b888a9d286cd499a8375d1f2ebc2722a676822d9",
          "message": "Fixed formatting (#1118)\n\n* Fixed formatting\r\n\r\n* Formatting master",
          "timestamp": "2023-07-18T14:53:30+01:00",
          "tree_id": "0c3ea68a94a74d7bb6098427764f0d7031400205",
          "url": "https://github.com/Pometry/Raphtory/commit/b888a9d286cd499a8375d1f2ebc2722a676822d9"
        },
        "date": 1689688850028,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 809076,
            "range": "± 2158",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1457074,
            "range": "± 4151",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1496541,
            "range": "± 3356",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1279810,
            "range": "± 71836",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2360777,
            "range": "± 56840",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ef8d5b107feeae0b0b332de2e6fa7d67edcfa01b",
          "message": "Feature/layering in pandas (#1124)\n\n* Adding layers to load_edges_from_num_iter\r\n\r\n* Added explit layer setting\r\n\r\n* Fixed test\r\n\r\n* Fix test\r\n\r\n* fix tests",
          "timestamp": "2023-07-18T17:54:56+01:00",
          "tree_id": "e0aa53c12bc364aca4fb2459b319b242942af640",
          "url": "https://github.com/Pometry/Raphtory/commit/ef8d5b107feeae0b0b332de2e6fa7d67edcfa01b"
        },
        "date": 1689699661632,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 757948,
            "range": "± 2624",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1318471,
            "range": "± 4129",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1360941,
            "range": "± 6759",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 966759,
            "range": "± 72088",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1851300,
            "range": "± 66206",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c43207a451a7c8e7b4d4d93f2e1f6c3e30428ac7",
          "message": "Adds a format checker to the workflows, which fails if the code has not been formatted, adds a dev folder with instructions to setup a nightly env + point the git hooks to the local folder (#1126)\n\n* fixs docs\r\n\r\n* fixed spelling, be me, cant spell\r\n\r\n* :boom: add needs check to prevent premature startup\r\n\r\n* :zap: prevent failure\r\n\r\n* testing hook\r\n\r\n* add bootstrap file, hooks and instructions\r\n\r\n* testing hook",
          "timestamp": "2023-07-20T09:36:42+01:00",
          "tree_id": "5e04f8b360f76c00ba864522656aae5b41cd3e8b",
          "url": "https://github.com/Pometry/Raphtory/commit/c43207a451a7c8e7b4d4d93f2e1f6c3e30428ac7"
        },
        "date": 1689842567772,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 694736,
            "range": "± 1929",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1231764,
            "range": "± 4020",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1263134,
            "range": "± 3602",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1053762,
            "range": "± 82568",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1918835,
            "range": "± 79779",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ba8daba4e23ae19c2f1d97537a4c2b9f633c6100",
          "message": "Algorithms now have their own result object with many features (#1105)\n\n* initial version of creating a polaris df from polars and fixing all the str& their dodgy prelude breaks\r\n\r\n* hello algorithm result, how do you do you?\r\n\r\n* fix tests for pr\r\n\r\n* checkin\r\n\r\n* hits fxhashmap you will not be missed 🫡\r\n\r\n* checkin with my code :salute:\r\n\r\n* at this point only god knows how it works. added tests for all types we currently support in our algorithms\r\n\r\n* add cargo missing ordered float package\r\n\r\n* fix tests\r\n\r\n* all algos done except three node motifs\r\n\r\n* implement debug formatter for algorithm result object to pass last failed test\r\n\r\n* 😈Generic type the input as well, why not😈\r\n\r\n* convert the three node motifs\r\n\r\n* added python support for algorithm result object\r\n\r\n* add default args to py fns\r\n\r\n* fix all pytests\r\n\r\n* add tests for new pyalgo fns\r\n\r\n* add tests, add to_df fn\r\n\r\n* add docs\r\n\r\n* fix graph ql issues during compiling\r\n\r\n* add docstrings\r\n\r\n* fixs docs\r\n\r\n* :art: fixed most issues\r\n\r\n* fmt\r\n\r\n* add docs\r\n\r\n* cargo fmt",
          "timestamp": "2023-07-20T11:16:25+01:00",
          "tree_id": "3d30db35ae9bf1a02af99228bf3542580b1a96a5",
          "url": "https://github.com/Pometry/Raphtory/commit/ba8daba4e23ae19c2f1d97537a4c2b9f633c6100"
        },
        "date": 1689848606824,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 804598,
            "range": "± 2981",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1451816,
            "range": "± 16983",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1486463,
            "range": "± 5533",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1235235,
            "range": "± 98614",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2207889,
            "range": "± 88241",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "dd0917ae4a69c73d9f5448224b932b38bfc5d358",
          "message": "Feature/python graphql (#1095)\n\n* Commit for merging with search\r\n\r\n* Messing with async\r\n\r\n* temp\r\n\r\n* Running from files working - running from dict next\r\n\r\n* Fixed segfault with run_from_dict\r\n\r\n* Merged and fixed issues\r\n\r\n* Hiding async bits away\r\n\r\n* Added port\r\n\r\n* Running the server in the background\r\n\r\n* Added a proper health check\r\n\r\n* Basic documentation\r\n\r\n* Added docs\r\n\r\n* Added tests\r\n\r\n* fmt\r\n\r\n* revert file name change\r\n\r\n* Added dependencies\r\n\r\n* Fixed tests for windows",
          "timestamp": "2023-07-20T16:33:24+01:00",
          "tree_id": "e7a2be05cb9cff02719879066b9e364fad87da82",
          "url": "https://github.com/Pometry/Raphtory/commit/dd0917ae4a69c73d9f5448224b932b38bfc5d358"
        },
        "date": 1689867662001,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 802105,
            "range": "± 42163",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1492904,
            "range": "± 75780",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1560280,
            "range": "± 105853",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1482571,
            "range": "± 256413",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2341876,
            "range": "± 333987",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "543b733cfffc581c9ddc0c84cfe2fab360fc52a7",
          "message": "improved formatting git hook (#1133)",
          "timestamp": "2023-07-24T16:16:37+01:00",
          "tree_id": "04c93460a9d44981bc60b2bbdad79971ce49238c",
          "url": "https://github.com/Pometry/Raphtory/commit/543b733cfffc581c9ddc0c84cfe2fab360fc52a7"
        },
        "date": 1690212172376,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 713474,
            "range": "± 3923",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1260200,
            "range": "± 2929",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1292916,
            "range": "± 4836",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1105619,
            "range": "± 99571",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1993137,
            "range": "± 123101",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "880b0d691d1abbdf42147b999be2137e9679796a",
          "message": "add hits to python and remove window (#1130)\n\n* add hits to python and remove window\r\n\r\n* :rocket: clean final raphtory warning\r\n\r\n* Update lib.rs",
          "timestamp": "2023-07-24T17:53:49+01:00",
          "tree_id": "ceb6685a12775a2b320ce0e42223a626fb12f828",
          "url": "https://github.com/Pometry/Raphtory/commit/880b0d691d1abbdf42147b999be2137e9679796a"
        },
        "date": 1690218254291,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 724339,
            "range": "± 39618",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1413494,
            "range": "± 103766",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1418293,
            "range": "± 69946",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1123901,
            "range": "± 127501",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2001021,
            "range": "± 79758",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "4599890+iamsmkr@users.noreply.github.com",
            "name": "Shivam Kapoor",
            "username": "iamsmkr"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2eab2389b2cd9ca8f37ec7bf45730e553b8277cf",
          "message": "Feature/expanded edges (#1132)\n\n* impl expanded edges\r\n\r\n* impl graph expanded edges\r\n\r\n* cargo fmt\r\n\r\n* fix issue with edges filtering\r\n\r\n* fix typo",
          "timestamp": "2023-07-25T09:31:02+01:00",
          "tree_id": "f94b12eafa41eac667075136fdd77391d538ed4d",
          "url": "https://github.com/Pometry/Raphtory/commit/2eab2389b2cd9ca8f37ec7bf45730e553b8277cf"
        },
        "date": 1690274226354,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 758587,
            "range": "± 8821",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1340825,
            "range": "± 1549",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1380299,
            "range": "± 2485",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 906408,
            "range": "± 66408",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1721212,
            "range": "± 53555",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "16bf23100dc30df7aba4f2806aab9d8d44b2b403",
          "message": "Properties (#1103)\n\n* cleanup\r\n\r\n* changes to rebase\r\n\r\n* skeleton for properties objects\r\n\r\n* Change properties API to enable dynamic boxing and start implementing some python wrappers\r\n\r\n* compiles but still work to do\r\n\r\n* add properties object for edges and some other improvements\r\n\r\n* implement properties for Graph\r\n\r\n* Fix errors and warnings after merge\r\n\r\n* minor cleanup of properties\r\n\r\n* fix the tests\r\n\r\n* add some python interface methods\r\n\r\n* start changing the properties so that their is a top-level properties object with temporal and static as sub-parts\r\n\r\n* Mostly working but still need to fix all the tests and python\r\n\r\n* core compiles again after refactoring of static/temporal properties, still need to update python/tests\r\n\r\n* update tests\r\n\r\n* start updating python\r\n\r\n* fix all compile errors\r\n\r\n* fix windowed property keys\r\n\r\n* fix warnings\r\n\r\n* python properties api\r\n\r\n* support for vectorised property access\r\n\r\n* actually add some methods for vectorised props\r\n\r\n* sort of works but python api could be cleaner\r\n\r\n* more python vectorised apis\r\n\r\n* even more vectorised operations\r\n\r\n* vectorised NestedTemporalProperty\r\n\r\n* implement vectorised static properties and start experimenting with __eq__ support\r\n\r\n* macro for implementing == for iterables\r\n\r\n* equality support nests now\r\n\r\n* completely remove macro-based python iterators as they have no advantage over the generic ones\r\n\r\n* fix semantics for Properties iterables\r\n\r\n* more cleanup but PropertiesIterable behaviour is inconsistent\r\n\r\n* start fixing inconsistencies for PropsIterable\r\n\r\n* update vertex test to reflect expected semantics\r\n\r\n* improved __eq__ for properties\r\n\r\n* fix static properties\r\n\r\n* start implementing comparisons for temporal properties\r\n\r\n* finish implementation for temporal properties and update all python tests\r\n\r\n* fix quickcheck test (properties at t0 still have a latest value at t1)\r\n\r\n* Fix merge issues and warnings\r\n\r\n* use lazy iterables everywhere in temporal properties and start cleaning up internal names\r\n\r\n* more cleanup\r\n\r\n* rename static/meta to constant\r\n\r\n* minor fixes and renames\r\n\r\n* start adding some python docs\r\n\r\n* remove unused argument\r\n\r\n* add more python docs\r\n\r\n* clean up some names\r\n\r\n* add missing reprs\r\n\r\n* doc cleanup and make signatures work",
          "timestamp": "2023-07-26T12:15:47+01:00",
          "tree_id": "ef058e7796a0dd01cc33d6852707d17ab5b2a23c",
          "url": "https://github.com/Pometry/Raphtory/commit/16bf23100dc30df7aba4f2806aab9d8d44b2b403"
        },
        "date": 1690370567169,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 822609,
            "range": "± 7413",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1470970,
            "range": "± 19531",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1505044,
            "range": "± 7008",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1207166,
            "range": "± 93825",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2233304,
            "range": "± 102159",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "006d15051260df0e3b44149661e34f4052802908",
          "message": "make docstring signatures work with type hints (#1137)\n\n* make docstring signatures work with type hints\r\n\r\n* add some comments to explain how this magic works",
          "timestamp": "2023-07-27T14:17:49+02:00",
          "tree_id": "84338746f063daaadb75c4ac84c252943a420c41",
          "url": "https://github.com/Pometry/Raphtory/commit/006d15051260df0e3b44149661e34f4052802908"
        },
        "date": 1690460640775,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 671344,
            "range": "± 3877",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1215708,
            "range": "± 97750",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1243746,
            "range": "± 4786",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1020130,
            "range": "± 88801",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1883669,
            "range": "± 93949",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b932685d360455e388094169ec64292493e08b3d",
          "message": "add exploded in, out, all, edges to graphql (#1141)",
          "timestamp": "2023-07-28T11:14:52+01:00",
          "tree_id": "f874a16969b0ee9dccc686d101dcb30bca26e59a",
          "url": "https://github.com/Pometry/Raphtory/commit/b932685d360455e388094169ec64292493e08b3d"
        },
        "date": 1690539714000,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 796508,
            "range": "± 20634",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1448291,
            "range": "± 9111",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1483926,
            "range": "± 26949",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1087441,
            "range": "± 123207",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2206041,
            "range": "± 82576",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e0f53e8ce1dfa06490f302fd0b05a24536e9f98f",
          "message": "Bump certifi from 2023.5.7 to 2023.7.22 in /docs (#1136)\n\nBumps [certifi](https://github.com/certifi/python-certifi) from 2023.5.7 to 2023.7.22.\r\n- [Commits](https://github.com/certifi/python-certifi/compare/2023.05.07...2023.07.22)\r\n\r\n---\r\nupdated-dependencies:\r\n- dependency-name: certifi\r\n  dependency-type: direct:production\r\n...\r\n\r\nSigned-off-by: dependabot[bot] <support@github.com>\r\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>\r\nCo-authored-by: Haaroon Y <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-07-31T11:11:13+01:00",
          "tree_id": "8a613e5fabe37b513407d528b255b1608d26df9a",
          "url": "https://github.com/Pometry/Raphtory/commit/e0f53e8ce1dfa06490f302fd0b05a24536e9f98f"
        },
        "date": 1690798647552,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 666066,
            "range": "± 2887",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1219600,
            "range": "± 3907",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1248456,
            "range": "± 3398",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1012666,
            "range": "± 85101",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1847223,
            "range": "± 82367",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "2404621+fabianmurariu@users.noreply.github.com",
            "name": "Fabian Murariu",
            "username": "fabianmurariu"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a01ee30e42314d4783479b813e02e436eeb743cd",
          "message": "Multiple layers (#1120)\n\n* cleanup\r\n\r\n* changes to rebase\r\n\r\n* skeleton for properties objects\r\n\r\n* Change properties API to enable dynamic boxing and start implementing some python wrappers\r\n\r\n* compiles but still work to do\r\n\r\n* add properties object for edges and some other improvements\r\n\r\n* implement properties for Graph\r\n\r\n* Fix errors and warnings after merge\r\n\r\n* minor cleanup of properties\r\n\r\n* fix the tests\r\n\r\n* add some python interface methods\r\n\r\n* start changing the properties so that their is a top-level properties object with temporal and static as sub-parts\r\n\r\n* Mostly working but still need to fix all the tests and python\r\n\r\n* core compiles again after refactoring of static/temporal properties, still need to update python/tests\r\n\r\n* update tests\r\n\r\n* start updating python\r\n\r\n* fix all compile errors\r\n\r\n* fix windowed property keys\r\n\r\n* fix warnings\r\n\r\n* python properties api\r\n\r\n* support for vectorised property access\r\n\r\n* actually add some methods for vectorised props\r\n\r\n* sort of works but python api could be cleaner\r\n\r\n* more python vectorised apis\r\n\r\n* even more vectorised operations\r\n\r\n* vectorised NestedTemporalProperty\r\n\r\n* implement vectorised static properties and start experimenting with __eq__ support\r\n\r\n* macro for implementing == for iterables\r\n\r\n* equality support nests now\r\n\r\n* completely remove macro-based python iterators as they have no advantage over the generic ones\r\n\r\n* fix semantics for Properties iterables\r\n\r\n* more cleanup but PropertiesIterable behaviour is inconsistent\r\n\r\n* start fixing inconsistencies for PropsIterable\r\n\r\n* update vertex test to reflect expected semantics\r\n\r\n* attempt to figure out the implications\r\n\r\n* start adding multiple layers at the store level\r\n\r\n* implemented low level multiple layer support 15 tests failing\r\n\r\n* change multiple layers edge_tuples to avoid duplicate edges\r\n\r\n* surface the multi layer API changes to the graph traits\r\n\r\n* lift the multiple layers up one level\r\n\r\n* core rust 2 tests still failing\r\n\r\n* issues with edge_tuples\r\n\r\n* need to find a way to filter edges by time\r\n\r\n* all test pass, but materialize is known to be broken\r\n\r\n* all rust test pass\r\n\r\n* added layer_ids on all functions concerning edges\r\n\r\n* implemented TimeSemantics for LayeredGraph\r\n\r\n* rust tests pass and added edge.layers in python\r\n\r\n* python tests pass\r\n\r\n* fix vertex subgraph after rebase\r\n\r\n* cargo fmt\r\n\r\n* improved __eq__ for properties\r\n\r\n* fix static properties\r\n\r\n* remove some useless bits\r\n\r\n* start implementing comparisons for temporal properties\r\n\r\n* finish implementation for temporal properties and update all python tests\r\n\r\n* fix quickcheck test (properties at t0 still have a latest value at t1)\r\n\r\n* Fix merge issues and warnings\r\n\r\n* use lazy iterables everywhere in temporal properties and start cleaning up internal names\r\n\r\n* more cleanup\r\n\r\n* rename static/meta to constant\r\n\r\n* minor fixes and renames\r\n\r\n* Make getting a layer more friendly\r\n\r\n* add test to confirm materialize is broken\r\n\r\n* more improvements to api\r\n\r\n* add test to confirm static properties are broken\r\n\r\n* clean up the remaining `into`s in the layer api\r\n\r\n* start trying to fix properties\r\n\r\n* build infrastructure make temporal property merging possible\r\n\r\n* don't do this\r\n\r\n* Revert \"don't do this\"\r\n\r\nc2e298ee920beb99b45a749bb694b4ab4acaae57\r\n\r\n* everything compiles again\r\n\r\n* clean up layering\r\n\r\n* fix layer refining\r\n\r\n* fix multilayer property view\r\n\r\n* fix static properties\r\n\r\n* implement materialize again\r\n\r\n* fix warnings and clean up property list\r\n\r\n* start adding List and Map to TProp\r\n\r\n* this was fixed\r\n\r\n* this is not used\r\n\r\n* add List and Map to TProp\r\n\r\n* remove stray println that got reintroduced\r\n\r\n* clean up format\r\n\r\n* cleanup and fixes\r\n\r\n* Rename AsProp to IntoProp to fit rust conventions and add specialised converters for maps and lists to avoid ambiguous implementation errors due to rust ignoring associated types\r\n\r\n* add more tests for edge layer properties\r\n\r\n* fix merge issue\r\n\r\n* layered graph can inherit TimeSemantics again\r\n\r\n---------\r\n\r\nCo-authored-by: Lucas Jeub <lucas.jeub@pometry.com>",
          "timestamp": "2023-08-02T11:29:33+02:00",
          "tree_id": "707e39945a57ec8f2a4000eec1fbc0789a57595d",
          "url": "https://github.com/Pometry/Raphtory/commit/a01ee30e42314d4783479b813e02e436eeb743cd"
        },
        "date": 1690969035772,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 719716,
            "range": "± 36519",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1351095,
            "range": "± 54061",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1427490,
            "range": "± 93290",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1329400,
            "range": "± 205652",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2193428,
            "range": "± 482355",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a866fcab93382049905b109bed50942e9cb24b70",
          "message": "Fix __getitem__ for python TemporalProperties (#1147)\n\n* add test to confirm bug\r\n\r\n* Fix __getitem__ for python TemporalProperties",
          "timestamp": "2023-08-02T15:02:34+02:00",
          "tree_id": "9e027da135ee93556ff0b8e1be2ca2328e3a67ff",
          "url": "https://github.com/Pometry/Raphtory/commit/a866fcab93382049905b109bed50942e9cb24b70"
        },
        "date": 1690981486264,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 790343,
            "range": "± 33501",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1424119,
            "range": "± 53877",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1497017,
            "range": "± 76439",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1437502,
            "range": "± 269788",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2308307,
            "range": "± 197878",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "747a943fb39b9167576204d98f5917fc96a1db14",
          "message": "Feature/multi edge support (#1149)\n\n* add secondary index to support mutltiple edges with same time stamp\r\n\r\n* fix range bug for TimeIndexEntry\r\n\r\n* Vertex timestamps shouldn't have secondary indices\r\n\r\n* IndexedGraph needs to implement GraphViewOps\r\n\r\n* clean up uses of VertexRef\r\n\r\n* fix test due to api changes\r\n\r\n* IndexedGraph needs to support IntoDynamic\r\n\r\n* fix property values for exploded edges at same time point\r\n\r\n* make Edgelist properties api consistent with other places\r\n\r\n* add test for exploding multilayer edge\r\n\r\n* fix explode for edges\r\n\r\n* edge should not take layer name as input any more\r\n\r\n* fix edge.layers()\r\n\r\n* add exploded edge count to example\r\n\r\n* fix FromPyObject for Graph and add support for list and map properties in python\r\n\r\n* test graph, list, and map properties\r\n\r\n* rename _t methods on edge to _exploded\r\n\r\n* remove outdated comment\r\n\r\n* quick fix for indexing of list/map\r\n\r\n* Change add_edge such that it returns an edge view and fix deadlocks because of non-recursive reads\r\n\r\n* reduce test runtime",
          "timestamp": "2023-08-08T14:04:28+01:00",
          "tree_id": "3334ab5e6eb9ab94cd1537dcc3ffa498e7456ca4",
          "url": "https://github.com/Pometry/Raphtory/commit/747a943fb39b9167576204d98f5917fc96a1db14"
        },
        "date": 1691500354188,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 874045,
            "range": "± 155594",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1344948,
            "range": "± 67968",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1357803,
            "range": "± 62596",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1358490,
            "range": "± 146415",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2137319,
            "range": "± 202088",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "47283e2db552e913566a082ca3b6d2d1177f72ce",
          "message": "Bug/unsafe layer panic (#1166)\n\n* add reproducing python test\r\n\r\n* add reproducing rust test\r\n\r\n* remove unsafe_layer completely and fix constant property retrieval for edges",
          "timestamp": "2023-08-09T13:13:48+01:00",
          "tree_id": "56c93357353e36856192eadc7caa3726e4c9e95b",
          "url": "https://github.com/Pometry/Raphtory/commit/47283e2db552e913566a082ca3b6d2d1177f72ce"
        },
        "date": 1691583616776,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 721067,
            "range": "± 10727",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1068091,
            "range": "± 4809",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1102477,
            "range": "± 13917",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1245624,
            "range": "± 98103",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1887537,
            "range": "± 83945",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a105bafe3f600b45546932d5d625411b3a89e2c5",
          "message": "Added constant props to pandas loaders (#1165)\n\n* Added vertex type arguments to the pandas loader and tests for layers/types in pandas loader\r\n\r\n* Changed to static props\r\n\r\n* Added functions for adding constant props only\r\n\r\n* fmt\r\n\r\n* added tests for static props\r\n\r\n* Finalised tests",
          "timestamp": "2023-08-09T14:43:37+01:00",
          "tree_id": "b3aade65e1baa3045dc2775bd26ab4c22f800b64",
          "url": "https://github.com/Pometry/Raphtory/commit/a105bafe3f600b45546932d5d625411b3a89e2c5"
        },
        "date": 1691588986843,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 768664,
            "range": "± 2357",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1134517,
            "range": "± 2833",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1187745,
            "range": "± 5918",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1041142,
            "range": "± 64482",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1685236,
            "range": "± 68788",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "1a0218901bd7ae89415d4107f5ec2ddcf88aad8b",
          "message": "Release v0.5.0 (#1167)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-08-09T14:53:33+01:00",
          "tree_id": "797476b4fd9057d1c6c6d610ec49d9dbe7af502e",
          "url": "https://github.com/Pometry/Raphtory/commit/1a0218901bd7ae89415d4107f5ec2ddcf88aad8b"
        },
        "date": 1691589672861,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 740640,
            "range": "± 52647",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1184880,
            "range": "± 74875",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1318827,
            "range": "± 74182",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1302149,
            "range": "± 154528",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1849554,
            "range": "± 147749",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3ad0c02eef15dab8d44ea5fac0c6989b55c61475",
          "message": "Fixed example in README.md for 0.5.0",
          "timestamp": "2023-08-09T15:11:38+01:00",
          "tree_id": "e51f4b69d7375599a8b447ef739fe703cf30cea2",
          "url": "https://github.com/Pometry/Raphtory/commit/3ad0c02eef15dab8d44ea5fac0c6989b55c61475"
        },
        "date": 1691590545426,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 769455,
            "range": "± 5592",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1132271,
            "range": "± 4442",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1183559,
            "range": "± 4801",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1012431,
            "range": "± 61865",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1587026,
            "range": "± 51766",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "f9bff518105a7373ce6441097ab68812723cd131",
          "message": "Removed armv7 from python release pipeline",
          "timestamp": "2023-08-09T15:17:46+01:00",
          "tree_id": "de5ead8e3ebd3ce3e2f7532b6b723b4787a2b336",
          "url": "https://github.com/Pometry/Raphtory/commit/f9bff518105a7373ce6441097ab68812723cd131"
        },
        "date": 1691590805191,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 692024,
            "range": "± 2907",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1064494,
            "range": "± 7302",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1104435,
            "range": "± 3676",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1057846,
            "range": "± 99759",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1615776,
            "range": "± 95358",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33124479+narnolddd@users.noreply.github.com",
            "name": "Naomi Arnold",
            "username": "narnolddd"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "057df89c4682356d8ccfbe33cbbe84a40949d1dd",
          "message": "Feature kcore (#1123)\n\n* add k core algorithm ahead of triangle optimisations\r\n\r\n* optimisations to triangle_count\r\n\r\n* something not right\r\n\r\n* add test for triangle count on subgraph\r\n\r\n* fix the issue with compute and subgraphs\r\n\r\n* add k core algorithm ahead of triangle optimisations\r\n\r\n* optimisations to triangle_count\r\n\r\n* something not right\r\n\r\n* add test\r\n\r\n* triangle motifs first run\r\n\r\n* almost working just got a borrow issue\r\n\r\n* fixed borrow prob\r\n\r\n* global works, local is mega busted\r\n\r\n* test 4 global\r\n\r\n* working version up to events happening at same time\r\n\r\n* try fix fmt issue\r\n\r\n* formatting changes\r\n\r\n* fix unresolved ref\r\n\r\n* expose to python and rename classes\r\n\r\n* update 2 new edge api\r\n\r\n* add mini optimisation\r\n\r\n* fn for returning vertex subgraph as well as the set\r\n\r\n* simplification of map filter\r\n\r\n---------\r\n\r\nCo-authored-by: Lucas Jeub <lucas.jeub@pometry.com>\r\nCo-authored-by: Fabian Murariu <murariu.fabian@gmail.com>",
          "timestamp": "2023-08-11T11:48:43+01:00",
          "tree_id": "ea2bc1349692379b110473c9522cf08aadf9383e",
          "url": "https://github.com/Pometry/Raphtory/commit/057df89c4682356d8ccfbe33cbbe84a40949d1dd"
        },
        "date": 1691751369644,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 782482,
            "range": "± 18752",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1232917,
            "range": "± 58677",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1356256,
            "range": "± 69289",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1357766,
            "range": "± 191967",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1889255,
            "range": "± 126184",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "09ae218d2298e006f21d0e57b7dde809525846a1",
          "message": "GQL demo notes (#1170)\n\n* graph ql demo\r\n\r\n* graph ql demo\r\n\r\n* graph ql demo\r\n\r\n* graph ql demo",
          "timestamp": "2023-08-11T16:11:40+01:00",
          "tree_id": "38ce2d22146e3a7888eb68f6667c5c43aa5b3d0a",
          "url": "https://github.com/Pometry/Raphtory/commit/09ae218d2298e006f21d0e57b7dde809525846a1"
        },
        "date": 1691766837292,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 826810,
            "range": "± 13324",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1260639,
            "range": "± 30961",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1298517,
            "range": "± 11044",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1205610,
            "range": "± 106179",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1887824,
            "range": "± 107570",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c5a01c23916799de260f6ef96838484e4eaa72d6",
          "message": "Bug/performance (#1169)\n\n* fix performance regression in `active` method\r\n\r\n* Make window etc. accept i64 if it is positive\r\n\r\n* add graph ops benchmark on lotr to base so we catch performance regressions earlier next time\r\n\r\n* fix Interval conversions",
          "timestamp": "2023-08-14T10:21:03+01:00",
          "tree_id": "778f9735114dccdeb995c398188447a6b68f5fd4",
          "url": "https://github.com/Pometry/Raphtory/commit/c5a01c23916799de260f6ef96838484e4eaa72d6"
        },
        "date": 1692005471901,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 778866,
            "range": "± 1330",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1155722,
            "range": "± 1797",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1207223,
            "range": "± 20748",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1042290,
            "range": "± 61987",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1640523,
            "range": "± 63595",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 10,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 97,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 83,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5401,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 90636,
            "range": "± 219",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 55752,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 242,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 161,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5906,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 64,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 13112,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 447020,
            "range": "± 75",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 56629,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 198,
            "range": "± 28",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 186,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5473,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 68,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 8957,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 321079,
            "range": "± 138",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fcbf0175dc5bce688dc9d1785ed71e4b53032fb3",
          "message": "Release v0.5.1 (#1176)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-08-14T11:04:15+01:00",
          "tree_id": "fd3fdb08dd4a40b89f2584677cfeea2df3be4b12",
          "url": "https://github.com/Pometry/Raphtory/commit/fcbf0175dc5bce688dc9d1785ed71e4b53032fb3"
        },
        "date": 1692008164228,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 833083,
            "range": "± 31854",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1347080,
            "range": "± 92156",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1316159,
            "range": "± 31395",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1379013,
            "range": "± 215711",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2260462,
            "range": "± 238518",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 9,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 117,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 103,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 30,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 23,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5923,
            "range": "± 348",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 112194,
            "range": "± 8504",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 74553,
            "range": "± 1636",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 287,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 206,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 7876,
            "range": "± 172",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 67,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 29,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15065,
            "range": "± 251",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 539467,
            "range": "± 11705",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 67280,
            "range": "± 1544",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 334,
            "range": "± 65",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 233,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 7089,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 83,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 26,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 10207,
            "range": "± 187",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 390019,
            "range": "± 10841",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "2bb06bb27fa46ce628d68957bdae70f9a3ed1145",
          "message": "Fixed reddit demo (#1180)\n\nfixed reddit demo",
          "timestamp": "2023-08-15T14:50:06+01:00",
          "tree_id": "d6ec688d45fa4d295630adfee1f688de1e14ec69",
          "url": "https://github.com/Pometry/Raphtory/commit/2bb06bb27fa46ce628d68957bdae70f9a3ed1145"
        },
        "date": 1692108110217,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 735995,
            "range": "± 51881",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1310597,
            "range": "± 137023",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1235919,
            "range": "± 49843",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1567372,
            "range": "± 237839",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2323622,
            "range": "± 315877",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 9,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 106,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 99,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 29,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 23,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5582,
            "range": "± 676",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 105751,
            "range": "± 6954",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 68992,
            "range": "± 2181",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 298,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 193,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 7252,
            "range": "± 143",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 74,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15067,
            "range": "± 430",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 537305,
            "range": "± 15934",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 65150,
            "range": "± 5106",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 323,
            "range": "± 47",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 225,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 7331,
            "range": "± 307",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 88,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 27,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 10624,
            "range": "± 487",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 393954,
            "range": "± 9689",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "6be52dfb05debeb31670eab21d9116938954bdfa",
          "message": "GraphQl send graph (#1181)\n\n* start work on supporting sending graphs over graphql\r\n\r\n* implement graph upload\r\n\r\n* add test\r\n\r\n* change save and load to always use a materialised graph (so the bincode includes the graph type)\r\n\r\n* simplify the test",
          "timestamp": "2023-08-16T10:50:11+01:00",
          "tree_id": "79798ab742dcdd647d80c252f85e310c31fc5d26",
          "url": "https://github.com/Pometry/Raphtory/commit/6be52dfb05debeb31670eab21d9116938954bdfa"
        },
        "date": 1692180068179,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 828035,
            "range": "± 3305",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1274669,
            "range": "± 7383",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1315262,
            "range": "± 4551",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1239344,
            "range": "± 104482",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2068415,
            "range": "± 89048",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 8,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 109,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 97,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 23,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 6106,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 114792,
            "range": "± 121",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 64975,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 294,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 195,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 7903,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 74,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15924,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 511442,
            "range": "± 154",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 62134,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 308,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 222,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 7566,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 92,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 11008,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 368502,
            "range": "± 95",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "e38933ffcefadb461f26d4fe85b5117655e66795",
          "message": "Release v0.5.2 (#1184)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-08-16T12:27:23+01:00",
          "tree_id": "cc2068f5d879562860844f16a3afa3e5ecdc2869",
          "url": "https://github.com/Pometry/Raphtory/commit/e38933ffcefadb461f26d4fe85b5117655e66795"
        },
        "date": 1692185871970,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 771059,
            "range": "± 25644",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1192163,
            "range": "± 37646",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1187560,
            "range": "± 47108",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1133757,
            "range": "± 121715",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1807520,
            "range": "± 88332",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 8,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 99,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 88,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 23,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 20,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5423,
            "range": "± 199",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 107274,
            "range": "± 3587",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 57040,
            "range": "± 1005",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 246,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 170,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6984,
            "range": "± 97",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 67,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 23,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 14048,
            "range": "± 730",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 458756,
            "range": "± 12500",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 53516,
            "range": "± 583",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 240,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 176,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6281,
            "range": "± 185",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 77,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 24,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 10059,
            "range": "± 112",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 309443,
            "range": "± 11809",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "49699333+dependabot[bot]@users.noreply.github.com",
            "name": "dependabot[bot]",
            "username": "dependabot[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "d6fb05ed9bfe80a885ebfe0ca120e969ced068b0",
          "message": "Bump tornado from 6.3.2 to 6.3.3 in /docs (#1179)\n\nBumps [tornado](https://github.com/tornadoweb/tornado) from 6.3.2 to 6.3.3.\r\n- [Changelog](https://github.com/tornadoweb/tornado/blob/master/docs/releases.rst)\r\n- [Commits](https://github.com/tornadoweb/tornado/compare/v6.3.2...v6.3.3)\r\n\r\n---\r\nupdated-dependencies:\r\n- dependency-name: tornado\r\n  dependency-type: direct:production\r\n...\r\n\r\nSigned-off-by: dependabot[bot] <support@github.com>\r\nCo-authored-by: dependabot[bot] <49699333+dependabot[bot]@users.noreply.github.com>",
          "timestamp": "2023-08-16T17:41:14+01:00",
          "tree_id": "87d5e967b5b80f8751c5ca7789ee170e1edd1c8b",
          "url": "https://github.com/Pometry/Raphtory/commit/d6fb05ed9bfe80a885ebfe0ca120e969ced068b0"
        },
        "date": 1692204659511,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 700137,
            "range": "± 3462",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1068953,
            "range": "± 2392",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1107012,
            "range": "± 3821",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1117672,
            "range": "± 101861",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1684852,
            "range": "± 85467",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 7,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 91,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 81,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 19,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5091,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 94173,
            "range": "± 96",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 53859,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 227,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 167,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6333,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 65,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 12842,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 425752,
            "range": "± 601",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 52078,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 262,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 183,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5997,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 68,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 8909,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 306541,
            "range": "± 42",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ed93c36258dcdee8ef33d1f3062f206e638227f8",
          "message": "roundtrip support for sending and receiving graphs using base64-encoding (#1182)\n\n* roundtrip support for sending and receiving graphs using base64-encoded strings\r\n\r\n* print response for failed test\r\n\r\n* fix the test\r\n\r\n* rename receive function",
          "timestamp": "2023-08-17T10:03:26+01:00",
          "tree_id": "98a222a9aae19811de0a9d8c2f4b37a6e11ddc10",
          "url": "https://github.com/Pometry/Raphtory/commit/ed93c36258dcdee8ef33d1f3062f206e638227f8"
        },
        "date": 1692263680743,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 750868,
            "range": "± 26569",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1240231,
            "range": "± 141429",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1202832,
            "range": "± 48503",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1281405,
            "range": "± 186617",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1989175,
            "range": "± 216841",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 8,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 100,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 89,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 26,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 21,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5179,
            "range": "± 366",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 100304,
            "range": "± 6344",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 64812,
            "range": "± 1568",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 272,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 186,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 7030,
            "range": "± 311",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 75,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 24,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 13874,
            "range": "± 876",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 516179,
            "range": "± 18564",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 59941,
            "range": "± 1257",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 240,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 217,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6722,
            "range": "± 137",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 84,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 24,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9794,
            "range": "± 145",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 351822,
            "range": "± 14951",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "64b95dec6781281154fe7a996f459e47871b435b",
          "message": "Added GraphQL Client  (#1185)\n\n* moved graphql tests to own folder, renamed graphlql to graphlqlserver, added a new graphqlclient with predefined mutation queries, added tests except for upload, added new [gql dep to toml\r\n\r\n* sorted the test result\r\n\r\n* add support for upload and add tests, format\r\n\r\n* better docs",
          "timestamp": "2023-08-17T18:26:37+01:00",
          "tree_id": "b867f19bdb13c509ce3a80a82037863223152da3",
          "url": "https://github.com/Pometry/Raphtory/commit/64b95dec6781281154fe7a996f459e47871b435b"
        },
        "date": 1692293557950,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 721341,
            "range": "± 55869",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1093990,
            "range": "± 2503",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1126032,
            "range": "± 2561",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1138913,
            "range": "± 116169",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1760135,
            "range": "± 124632",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 7,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 92,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 82,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 21,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 19,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5033,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 94446,
            "range": "± 177",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 53541,
            "range": "± 65",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 211,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 164,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6412,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 66,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 12906,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 426162,
            "range": "± 166",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 54148,
            "range": "± 790",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 211,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 165,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5922,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 68,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 22,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 8914,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 308757,
            "range": "± 172",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "a462cb039b14dc26d7eadd206a29c9c637e6253a",
          "message": "add layer fn (#1194)",
          "timestamp": "2023-08-18T10:20:37+01:00",
          "tree_id": "2a55f7ba0c9df930d86531df20678dc45b09d909",
          "url": "https://github.com/Pometry/Raphtory/commit/a462cb039b14dc26d7eadd206a29c9c637e6253a"
        },
        "date": 1692351036298,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 715171,
            "range": "± 3799",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1088674,
            "range": "± 4513",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1124780,
            "range": "± 3636",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1056328,
            "range": "± 85522",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1723653,
            "range": "± 79739",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 7,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 91,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 82,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 19,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5018,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 95002,
            "range": "± 313",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 53583,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 229,
            "range": "± 28",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 174,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6376,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 63,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 12841,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 428483,
            "range": "± 239",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 51924,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 251,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 170,
            "range": "± 33",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5905,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 56,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 8862,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 307598,
            "range": "± 85",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c5930ada9ec4613928663ac00f69e9f4b87a01f2",
          "message": "Update README.md",
          "timestamp": "2023-08-21T09:46:56+01:00",
          "tree_id": "0a80c584c68a8c975aa0fc11847902377ac09b7e",
          "url": "https://github.com/Pometry/Raphtory/commit/c5930ada9ec4613928663ac00f69e9f4b87a01f2"
        },
        "date": 1692608272447,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 803393,
            "range": "± 3822",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1255693,
            "range": "± 3125",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1310267,
            "range": "± 4979",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1246374,
            "range": "± 107490",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1985415,
            "range": "± 82318",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 10,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 112,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 101,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 28,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 6133,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 113164,
            "range": "± 1083",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 60056,
            "range": "± 107",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 312,
            "range": "± 33",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 181,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 7788,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 74,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15930,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 503443,
            "range": "± 1554",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 56517,
            "range": "± 370",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 301,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 211,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 7319,
            "range": "± 31",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 84,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 10872,
            "range": "± 60",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 358205,
            "range": "± 1626",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "ricopinazo@gmail.com",
            "name": "Pedro Rico Pinazo",
            "username": "ricopinazo"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c458c2bac40585c5d3ddebae4a5d21a047f9b8c4",
          "message": "add schema for layers and edges (#1196)\n\n* add schema for layers and edges\r\n\r\n* add docs for LayerSchema.name function\r\n\r\n* run cargo fmt\r\n\r\n* make input argument mut in merge_schemas\r\n\r\n* roll back changes in src/model/mod.rs",
          "timestamp": "2023-08-23T10:55:17+01:00",
          "tree_id": "0ce70e9afcd48574ec97ef40a4b419b7ae5ffd87",
          "url": "https://github.com/Pometry/Raphtory/commit/c458c2bac40585c5d3ddebae4a5d21a047f9b8c4"
        },
        "date": 1692785164125,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 802594,
            "range": "± 7318",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1255173,
            "range": "± 16256",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1319987,
            "range": "± 13397",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1252061,
            "range": "± 84542",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2059921,
            "range": "± 92346",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 10,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 112,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 100,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 28,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 6475,
            "range": "± 338",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 113880,
            "range": "± 5247",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 59837,
            "range": "± 97",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 283,
            "range": "± 35",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 205,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 7979,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 75,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15316,
            "range": "± 258",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 490167,
            "range": "± 4935",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 56648,
            "range": "± 601",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 320,
            "range": "± 55",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 199,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 7058,
            "range": "± 118",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 78,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 24,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 10563,
            "range": "± 147",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 357166,
            "range": "± 3061",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "58d8a08b207eed7b0ae7ab7ba2e2f254cb50af5a",
          "message": "Pandas loading bars (#1197)\n\n* tqdm working\r\n\r\n* cargo lock\r\n\r\n* Added to all pandas functions\r\n\r\n* fmt\r\n\r\n* Fixed test and warnings\r\n\r\n* fmt\r\n\r\n* Fixed warnings in grpahql test\r\n\r\n* trying a different len",
          "timestamp": "2023-08-23T13:52:42+01:00",
          "tree_id": "a01279c377c9bfd50f17ea828b57474552fa14dc",
          "url": "https://github.com/Pometry/Raphtory/commit/58d8a08b207eed7b0ae7ab7ba2e2f254cb50af5a"
        },
        "date": 1692795768205,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 669719,
            "range": "± 71229",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1054058,
            "range": "± 3753",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1100339,
            "range": "± 2034",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1124861,
            "range": "± 80737",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1723292,
            "range": "± 81997",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 8,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 95,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 84,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 23,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5061,
            "range": "± 24",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 93523,
            "range": "± 125",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 49628,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 225,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 159,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6558,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 65,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 13314,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 419667,
            "range": "± 110",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 47543,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 239,
            "range": "± 44",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 176,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6038,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 67,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9045,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 300836,
            "range": "± 501",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "9ca9166b455304d616b3ee817f5f416fdbca6b93",
          "message": "Small changes to docs before the big update (#1199)\n\n* Tided the lotr examples\r\n\r\n* Tidying lotr + structure\r\n\r\n* Removed overview till we write a nicer one\r\n\r\n* Updating the faq\r\n\r\n* Finalised this version of docs\r\n\r\n* Added a couple bits to the reddit example",
          "timestamp": "2023-08-23T16:00:37+01:00",
          "tree_id": "0986b0a08b0cd63684397dfcdc348a13b48bd058",
          "url": "https://github.com/Pometry/Raphtory/commit/9ca9166b455304d616b3ee817f5f416fdbca6b93"
        },
        "date": 1692803209859,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 814548,
            "range": "± 24477",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1272678,
            "range": "± 24692",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1341138,
            "range": "± 240820",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1497644,
            "range": "± 193258",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2217527,
            "range": "± 247764",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 10,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 120,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 111,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 30,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 26,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 6137,
            "range": "± 181",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 107421,
            "range": "± 5142",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 67621,
            "range": "± 1486",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 319,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 210,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 7800,
            "range": "± 106",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 65,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15523,
            "range": "± 338",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 532118,
            "range": "± 8562",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 62540,
            "range": "± 1037",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 253,
            "range": "± 62",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 232,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 7431,
            "range": "± 56",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 81,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 26,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 10821,
            "range": "± 136",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 386192,
            "range": "± 6064",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Alnaimi-@users.noreply.github.com",
            "name": "Alhamza Alnaimi",
            "username": "Alnaimi-"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fea9f6cb14ee45a7606d372e33dd986f649bfc94",
          "message": "Update README.md (#1201)\n\n* Update README.md\r\n\r\n* Update README.md",
          "timestamp": "2023-08-28T12:45:11+01:00",
          "tree_id": "d914be3d637dff960db39c4dc4801b4013839389",
          "url": "https://github.com/Pometry/Raphtory/commit/fea9f6cb14ee45a7606d372e33dd986f649bfc94"
        },
        "date": 1693223909550,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 712147,
            "range": "± 12869",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1176926,
            "range": "± 83294",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1179027,
            "range": "± 62760",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1054031,
            "range": "± 143850",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1715118,
            "range": "± 192971",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 9,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 105,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 92,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 25,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 23,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5551,
            "range": "± 329",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 97556,
            "range": "± 4327",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 62525,
            "range": "± 1457",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 263,
            "range": "± 39",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 188,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6981,
            "range": "± 134",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 74,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 24,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 14399,
            "range": "± 206",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 522624,
            "range": "± 12283",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 59758,
            "range": "± 1659",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 293,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 197,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6364,
            "range": "± 124",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 71,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 23,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9680,
            "range": "± 182",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 364320,
            "range": "± 18527",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "dulla2go@gmail.com",
            "name": "Abdullah Hasan",
            "username": "Dullaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b9870d74f07c6d8b09ca78d45950d31e60136c28",
          "message": "refactor(graphql): rename filters to use snake case naming (#1204)\n\nCo-authored-by: Abdullah Hasan <abdullah.hasan@payaut.com>",
          "timestamp": "2023-08-29T13:29:42+01:00",
          "tree_id": "7ffc766189c81f0dc5c19686d89c7aa1ecd69e7d",
          "url": "https://github.com/Pometry/Raphtory/commit/b9870d74f07c6d8b09ca78d45950d31e60136c28"
        },
        "date": 1693312811260,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 685491,
            "range": "± 1039",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1064717,
            "range": "± 4808",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1119008,
            "range": "± 14665",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1185531,
            "range": "± 127989",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1834473,
            "range": "± 110930",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 8,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 95,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 83,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 23,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5066,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 92554,
            "range": "± 644",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 49523,
            "range": "± 58",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 196,
            "range": "± 36",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 165,
            "range": "± 26",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6246,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 44,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 12791,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 419376,
            "range": "± 108",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 50027,
            "range": "± 74",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 248,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 173,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5929,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 70,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 8980,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 304162,
            "range": "± 55",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "552733fe08d4df43490e5cde0fbd277f37c1dd6c",
          "message": "fix property gets and add directed option to pyvis (#1206)\n\n* fix property gets and add directed option to pyvis\r\n\r\n* more tweaks to make this work better and pass unknown arguments down to pyvis directly\r\n\r\n* set notebook=True as default again (should fix docs and probably more sensibly default)",
          "timestamp": "2023-08-29T15:20:01+01:00",
          "tree_id": "7cf95a34f8e2350247dcd49c6c7a009d4236de5e",
          "url": "https://github.com/Pometry/Raphtory/commit/552733fe08d4df43490e5cde0fbd277f37c1dd6c"
        },
        "date": 1693319410119,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 684042,
            "range": "± 1701",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1062311,
            "range": "± 2283",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1113440,
            "range": "± 3108",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1087157,
            "range": "± 74394",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1721405,
            "range": "± 93559",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 8,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 96,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 84,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 23,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 21,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5059,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 91403,
            "range": "± 57",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 49760,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 242,
            "range": "± 27",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 157,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6082,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 65,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 23,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 12788,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 421864,
            "range": "± 169",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 47882,
            "range": "± 33",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 199,
            "range": "± 45",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 185,
            "range": "± 33",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5867,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 68,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 22,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 8853,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 302150,
            "range": "± 112",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "dulla2go@gmail.com",
            "name": "Abdullah Hasan",
            "username": "Dullaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "73df9cfcebedb38c673418bae14b6824ee37a0d6",
          "message": "feat(graphql): Extend property filtering to edges in graphql (#1203)\n\n* feat(graphql): Extend property filtering to edges in graphql\r\n\r\n* Fix importing\r\n\r\n---------\r\n\r\nCo-authored-by: Abdullah Hasan <abdullah.hasan@payaut.com>",
          "timestamp": "2023-08-29T15:47:50+01:00",
          "tree_id": "fac975ed7e96d694341fab566186d92c640344d5",
          "url": "https://github.com/Pometry/Raphtory/commit/73df9cfcebedb38c673418bae14b6824ee37a0d6"
        },
        "date": 1693321080445,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 754286,
            "range": "± 2442",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1138477,
            "range": "± 3108",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1194164,
            "range": "± 3011",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 975964,
            "range": "± 52740",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1576348,
            "range": "± 59525",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 11,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 96,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 83,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5325,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 89196,
            "range": "± 161",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 49644,
            "range": "± 188",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 218,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 147,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5720,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 64,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 12825,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 431985,
            "range": "± 2318",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 47293,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 221,
            "range": "± 38",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 170,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5378,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 67,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 8778,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 307721,
            "range": "± 490",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "dulla2go@gmail.com",
            "name": "Abdullah Hasan",
            "username": "Dullaz"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "669ec558337e3b9d6a6186d3a78f6a662a62a3c1",
          "message": "feat(graphql): allow searching for multiple property histories in one… (#1205)\n\n* feat(graphql): allow searching for multiple property histories in one query\r\n\r\n* Rework the way properties histories are fetched.\r\n\r\n---------\r\n\r\nCo-authored-by: Abdullah Hasan <abdullah.hasan@payaut.com>\r\nCo-authored-by: Ben Steer <b.a.steer@qmul.ac.uk>",
          "timestamp": "2023-08-30T09:43:14+01:00",
          "tree_id": "8c3171f829dd02a37aae26f9af4f85b268f8fdc4",
          "url": "https://github.com/Pometry/Raphtory/commit/669ec558337e3b9d6a6186d3a78f6a662a62a3c1"
        },
        "date": 1693385599524,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 673723,
            "range": "± 27991",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1053939,
            "range": "± 11422",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1104103,
            "range": "± 2811",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1162003,
            "range": "± 113633",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1750343,
            "range": "± 117097",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 8,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 95,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 83,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 23,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5065,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 94647,
            "range": "± 139",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 50777,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 231,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 164,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6532,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 73,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 13438,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 451464,
            "range": "± 205",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 47611,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 221,
            "range": "± 34",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 192,
            "range": "± 21",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6035,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 70,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9115,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 324768,
            "range": "± 83",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4ebbf00c1c554b30c5484d5ce22c01e7e4018222",
          "message": "Release v0.5.3 (#1208)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-08-30T10:05:57+01:00",
          "tree_id": "81c94565e589b7fbd1623f6251d41c052de043bc",
          "url": "https://github.com/Pometry/Raphtory/commit/4ebbf00c1c554b30c5484d5ce22c01e7e4018222"
        },
        "date": 1693386975593,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 749554,
            "range": "± 4375",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1139947,
            "range": "± 4649",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1194207,
            "range": "± 4431",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1037803,
            "range": "± 70495",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1692277,
            "range": "± 68713",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 11,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 96,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 83,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5309,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 91663,
            "range": "± 109",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 49276,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 219,
            "range": "± 32",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 170,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5987,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 62,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 12956,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 429244,
            "range": "± 904",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 47029,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 235,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 170,
            "range": "± 23",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5336,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 67,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 8779,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 316080,
            "range": "± 66",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b8294f452956260a4d831da0c52df90f3bf944d1",
          "message": "Performance improvements  (#1202)\n\n* always inline base\r\n\r\n* optimisations for degree\r\n\r\n* optimise find_edge\r\n\r\n* add None option to edge_refs\r\n\r\n* some extra inlines\r\n\r\n* attempted optimisation of edgerefs\r\n\r\n* remove option from underlying storage\r\n\r\n* randomly trying things to get it to run quicker\r\n\r\n* make num_edges parallel\r\n\r\n* improve has_edge\r\n\r\n* Implement iterators that avoid genawaiter\r\n\r\n* fix degree\r\n\r\n* add cleaner test to show broken num_edges on window for GraphWithDeletions\r\n\r\n* compiles but still failing some tests\r\n\r\n* fix last tests\r\n\r\n* eliminate some Arc clones\r\n\r\n* better way to create filter for windowed graph\r\n\r\n* add some helpful inlines\r\n\r\n* more inlines (not sure if useful)\r\n\r\n* replicate failing python test in rust\r\n\r\n* fix missing bounds check in into_layers\r\n\r\n* fix rebase and clean up warnings\r\n\r\n* remove optimisation that causes a race condition\r\n\r\n* layer_ids were not passed through properly\r\n\r\n* fix more warnings\r\n\r\n* more refactoring to get rid of Arc clones for speed\r\n\r\n* clean up warnings\r\n\r\n* More inlines and minor refactor\r\n\r\n* change remote and local to internal and external\r\n\r\n---------\r\n\r\nCo-authored-by: Fabian Murariu <murariu.fabian@gmail.com>",
          "timestamp": "2023-08-31T13:40:21+02:00",
          "tree_id": "c1dafd9c0bd0066526656c7bbf33dd3e32b11053",
          "url": "https://github.com/Pometry/Raphtory/commit/b8294f452956260a4d831da0c52df90f3bf944d1"
        },
        "date": 1693482639639,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 703773,
            "range": "± 1545",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1082116,
            "range": "± 9763",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1112322,
            "range": "± 4479",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1069059,
            "range": "± 128829",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1641659,
            "range": "± 117850",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 75,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 74,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 4975,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 32284,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 25854,
            "range": "± 221",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 106,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 76,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5710,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 63,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15064,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 244037,
            "range": "± 49",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 25801,
            "range": "± 166",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 98,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 76,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5630,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 67,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9727,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 161668,
            "range": "± 38",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c20fa06156b0981d830a768e0f97c6dae1028dd7",
          "message": "Changing property type now returns an error instead of silently ignoring the value (#1211)\n\n* changing property type now returns an error instead of silently ignoring the value\r\n\r\n* remove the TODO",
          "timestamp": "2023-09-01T09:29:44+01:00",
          "tree_id": "895f850712e92491238c0d7aa08859cd57989293",
          "url": "https://github.com/Pometry/Raphtory/commit/c20fa06156b0981d830a768e0f97c6dae1028dd7"
        },
        "date": 1693557654308,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 808324,
            "range": "± 2587",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1268656,
            "range": "± 8361",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1309045,
            "range": "± 5522",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1388493,
            "range": "± 122260",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2137134,
            "range": "± 130513",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 91,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 89,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 6034,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 39240,
            "range": "± 154",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 29769,
            "range": "± 1109",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 125,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 87,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6828,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 76,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 18037,
            "range": "± 128",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 292656,
            "range": "± 190",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 30488,
            "range": "± 195",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 138,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 88,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6685,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 83,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 11659,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 193777,
            "range": "± 978",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4c65240081fafa7715a67d7f2df1182aac50ed2d",
          "message": "Fix input argument for subgraph in python (#1218)\n\n* fix the input type for the python side subgraph method\r\n\r\n* add test",
          "timestamp": "2023-09-04T11:56:06+01:00",
          "tree_id": "3af9ee6015dd05d8e8f7ad8ecca7c250da453c5d",
          "url": "https://github.com/Pometry/Raphtory/commit/4c65240081fafa7715a67d7f2df1182aac50ed2d"
        },
        "date": 1693825578976,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 749259,
            "range": "± 2581",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1126213,
            "range": "± 7126",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1169840,
            "range": "± 5995",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 990816,
            "range": "± 53730",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1639838,
            "range": "± 60430",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 3,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 79,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 78,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5244,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 33469,
            "range": "± 763",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 28133,
            "range": "± 401",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 99,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 80,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5507,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 65,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15055,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 242426,
            "range": "± 144",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 27952,
            "range": "± 103",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 105,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 77,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5343,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 68,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9812,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 147389,
            "range": "± 93",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "6eb04fcce4a904aef2b6b86f9ef50840f1265b65",
          "message": "Updated docs to just be the python APIs (#1222)",
          "timestamp": "2023-09-04T13:10:08+01:00",
          "tree_id": "0d54fd3a402f47f6f1e4777079c242a68662491d",
          "url": "https://github.com/Pometry/Raphtory/commit/6eb04fcce4a904aef2b6b86f9ef50840f1265b65"
        },
        "date": 1693830024035,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 674464,
            "range": "± 2173",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1054284,
            "range": "± 2617",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1087567,
            "range": "± 3674",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1168993,
            "range": "± 90593",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1842930,
            "range": "± 106718",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 75,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 75,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5041,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 32768,
            "range": "± 40",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 26453,
            "range": "± 180",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 99,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 75,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5705,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 62,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15115,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 243573,
            "range": "± 76",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 26081,
            "range": "± 298",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 99,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 77,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5629,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 73,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9707,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 163859,
            "range": "± 38",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "911090a13fc819d07540290167d5352d914ab1b5",
          "message": "Release v0.5.4 (#1223)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-09-04T14:21:14+01:00",
          "tree_id": "6941c3c8d389f260fec13bc4ed3cba36b33757b0",
          "url": "https://github.com/Pometry/Raphtory/commit/911090a13fc819d07540290167d5352d914ab1b5"
        },
        "date": 1693834364250,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 841612,
            "range": "± 13794",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1295868,
            "range": "± 19073",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1335020,
            "range": "± 9921",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1406677,
            "range": "± 91535",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2152513,
            "range": "± 120797",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 90,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 88,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 6030,
            "range": "± 38",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 39644,
            "range": "± 104",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 31610,
            "range": "± 369",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 133,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 91,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6810,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 79,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 17719,
            "range": "± 34",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 291402,
            "range": "± 969",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 31139,
            "range": "± 1089",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 117,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 88,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6698,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 84,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 11475,
            "range": "± 66",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 193342,
            "range": "± 565",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "47d9b63456fbcc4d1ce1bdac61d1755252c4c481",
          "message": "sum weight algorithm + min/max/mean/median/average/count/len/sum features on edge properties  (#1200)\n\n* sum weight for a vertex algorithm\r\n\r\n* add in out and both\r\n\r\n* yugioh mode, implement pydirection, implement python algorithm + tests\r\n\r\n* add docs for python algo\r\n\r\n* implemented many functions upon the list, listlist, pyiterable, sum, count,\r\n\r\n* fixed getter for edges with small annotation, added sum for valuelist and valuelistlist, fixed internal error with empty result, fixed none appearing in empty lists when doing a sum\r\n\r\n* added more\r\n\r\n* median\r\n\r\n* remove median temporarily\r\n\r\n* lighter algorithms\r\n\r\n* fixed balance to be temporal\r\n\r\n* whats done is done\r\n\r\n* no more unwrap explosions\r\n\r\n* add min and max vaue and keys\r\n\r\n* min max key and value for algo result\r\n\r\n* add min, max, mean, average to propiterable\r\n\r\n* add min, max, mean, average, median, count, len to PyPropHistValueList + python tests\r\n\r\n* move test to its own code\r\n\r\n* add len, count, min, max to PyPropValueList + tests\r\n\r\n* added average + mean to PyPropValueList + tests\r\n\r\n* add len, count, min, max, mean, avg, median to PyPropValueList\r\n List+ tests\r\n\r\n* added len, count, min, max, mean, avg, median to PyTemporalProp+ tests\r\n\r\n* idk blank lines?\r\n\r\n* in algo result min max median now return a k,v pair option",
          "timestamp": "2023-09-05T15:47:38+01:00",
          "tree_id": "7f28f4b4f1765727406e8cc2801a70a4b71c9dfd",
          "url": "https://github.com/Pometry/Raphtory/commit/47d9b63456fbcc4d1ce1bdac61d1755252c4c481"
        },
        "date": 1693925896750,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 803464,
            "range": "± 18871",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1273188,
            "range": "± 19012",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1268944,
            "range": "± 23411",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1312065,
            "range": "± 114608",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2090434,
            "range": "± 148560",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 88,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 86,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 24,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5888,
            "range": "± 137",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 39552,
            "range": "± 1028",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 29878,
            "range": "± 226",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 122,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 86,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6893,
            "range": "± 25",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 80,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 18016,
            "range": "± 60",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 287595,
            "range": "± 1598",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 29108,
            "range": "± 348",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 124,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 88,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6551,
            "range": "± 101",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 78,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 11564,
            "range": "± 83",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 192122,
            "range": "± 2060",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "42b4beafd14071a19102b0adc60ed46f4cf3693f",
          "message": "Release v0.5.5 (#1227)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-09-05T16:15:42+01:00",
          "tree_id": "c6131ed3bc06f3ed4ea31ab5fb040a06096da31f",
          "url": "https://github.com/Pometry/Raphtory/commit/42b4beafd14071a19102b0adc60ed46f4cf3693f"
        },
        "date": 1693927609565,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 841809,
            "range": "± 22031",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1275662,
            "range": "± 37223",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1369740,
            "range": "± 52093",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1339710,
            "range": "± 133371",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2102766,
            "range": "± 155688",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 90,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 90,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5979,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 38847,
            "range": "± 171",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 30893,
            "range": "± 401",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 125,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 88,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6808,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 75,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 18021,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 289707,
            "range": "± 101",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 30496,
            "range": "± 638",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 135,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 90,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6717,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 83,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 11685,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 194504,
            "range": "± 93",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "33124479+narnolddd@users.noreply.github.com",
            "name": "Naomi Arnold",
            "username": "narnolddd"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "8f42868b3f1148a6b6c044aec5b98cb374cac9df",
          "message": "Feature/temporal edges (#1241)\n\n* basic temporal edges function\r\n\r\n* added python view\r\n\r\n* fixed format",
          "timestamp": "2023-09-06T10:07:57+01:00",
          "tree_id": "e051c9ff68d5032cd981de31ea52cc11fc38df71",
          "url": "https://github.com/Pometry/Raphtory/commit/8f42868b3f1148a6b6c044aec5b98cb374cac9df"
        },
        "date": 1693991869537,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 703063,
            "range": "± 1459",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1082978,
            "range": "± 1781",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1117637,
            "range": "± 2951",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1187884,
            "range": "± 116481",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1801708,
            "range": "± 99170",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 76,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 73,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 4976,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 32558,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 25562,
            "range": "± 431",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 110,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 73,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5588,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 63,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 14588,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 239735,
            "range": "± 79",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 25307,
            "range": "± 183",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 97,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 74,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5480,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 65,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9540,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 160509,
            "range": "± 384",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "483722393a78e74cd032201f9ad2c43e914598bc",
          "message": "Removed unwarp in getter for results (#1242)\n\n* Removed unwarp in getter for results\r\n\r\n* Added some tests to show it works",
          "timestamp": "2023-09-06T10:33:11+01:00",
          "tree_id": "a86a5252793dd67233f07bfd16e9788d621a40e3",
          "url": "https://github.com/Pometry/Raphtory/commit/483722393a78e74cd032201f9ad2c43e914598bc"
        },
        "date": 1693993456137,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 759318,
            "range": "± 63887",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1174733,
            "range": "± 52906",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1325606,
            "range": "± 70638",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1453816,
            "range": "± 262214",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 2229901,
            "range": "± 335159",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 87,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 86,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 25,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 24,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5566,
            "range": "± 417",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 37651,
            "range": "± 4831",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 35848,
            "range": "± 1374",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 134,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 99,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 7387,
            "range": "± 120",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 57,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 27,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 18148,
            "range": "± 308",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 324710,
            "range": "± 9153",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 35481,
            "range": "± 656",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 132,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 94,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6871,
            "range": "± 346",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 89,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 26,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 11531,
            "range": "± 319",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 217912,
            "range": "± 5416",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3aaeeec9f36adb6031e27a467fd3e42676bc4e23",
          "message": "Improved property additions api for vertices and edges (#1228)\n\n* new apis\r\n\r\n* wip refactor of add_edge_properties\r\n\r\n* remove old apis and update everything\r\n\r\n* fix semantics, add tests and cleanup\r\n\r\n* add layer_name and time methods for edge and list of edges where missing\r\n\r\n* minor cleanup\r\n\r\n* use internal ids for all the internal mutation methods to avoid costly unnecessary id lookups\r\n\r\n* fix the tests\r\n\r\n* fix the warnings\r\n\r\n* use enum_dispatch to make MaterializedGraph implement GraphViewOps\r\n\r\n* implement python wrappers for new property apis and update the tests\r\n\r\n* fix rebase error and warnings\r\n\r\n* fix test notebook and remove the outputs",
          "timestamp": "2023-09-06T13:20:56+01:00",
          "tree_id": "31cb5554e34e4cf83803bb34f7bdc4c25eb10cd1",
          "url": "https://github.com/Pometry/Raphtory/commit/3aaeeec9f36adb6031e27a467fd3e42676bc4e23"
        },
        "date": 1694003452205,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 682007,
            "range": "± 2022",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1026409,
            "range": "± 2878",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1074108,
            "range": "± 1596",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 898987,
            "range": "± 51849",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1529465,
            "range": "± 57577",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 78,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 78,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5183,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 33435,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 29802,
            "range": "± 398",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 99,
            "range": "± 6",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 77,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5592,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 64,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15448,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 249839,
            "range": "± 180",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 28987,
            "range": "± 241",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 99,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 77,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5396,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 64,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 10075,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 156099,
            "range": "± 153",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "committer": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Yousaf",
            "username": "Haaroon"
          },
          "distinct": true,
          "id": "48c0f329e02e73b9b2370f089e037af7c887bc79",
          "message": "Revert \"add property support for u8 and u16\"\n\nThis reverts commit 0519171e01c42db8dd590fd049ec80700c7bedd8.",
          "timestamp": "2023-09-06T15:49:14+01:00",
          "tree_id": "31cb5554e34e4cf83803bb34f7bdc4c25eb10cd1",
          "url": "https://github.com/Pometry/Raphtory/commit/48c0f329e02e73b9b2370f089e037af7c887bc79"
        },
        "date": 1694012388927,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 735634,
            "range": "± 6804",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1155711,
            "range": "± 33809",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1191893,
            "range": "± 12831",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1059200,
            "range": "± 92047",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1743048,
            "range": "± 111191",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 89,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 86,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5850,
            "range": "± 113",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 37746,
            "range": "± 459",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 32274,
            "range": "± 1400",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 119,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 87,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6901,
            "range": "± 50",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 76,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 17778,
            "range": "± 81",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 289896,
            "range": "± 1791",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 30026,
            "range": "± 251",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 117,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 91,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6695,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 78,
            "range": "± 18",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 11563,
            "range": "± 44",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 191342,
            "range": "± 922",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "Haaroon@users.noreply.github.com",
            "name": "Haaroon Y",
            "username": "Haaroon"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "13fd0cb9a246f3cb959b2e1cf23ecd5a5bbfe791",
          "message": "Added U8 and U16 property types  (#1248)\n\n* add property support for u8 and u16\r\n\r\n* added support for u8 and u16 prop types",
          "timestamp": "2023-09-06T22:15:31+01:00",
          "tree_id": "d5d496e7df5d39c66a62c126ed109d6254f35f73",
          "url": "https://github.com/Pometry/Raphtory/commit/13fd0cb9a246f3cb959b2e1cf23ecd5a5bbfe791"
        },
        "date": 1694035519964,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 622154,
            "range": "± 3029",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 977869,
            "range": "± 4298",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1017459,
            "range": "± 2565",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1038690,
            "range": "± 73641",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1725237,
            "range": "± 42154",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 76,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 73,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 4981,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 32404,
            "range": "± 29",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 27353,
            "range": "± 104",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 97,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 73,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6062,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 63,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15129,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 254505,
            "range": "± 83",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 26813,
            "range": "± 442",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 109,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 75,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5520,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 74,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9681,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 165984,
            "range": "± 87",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "fe1fad786a0a09ae2c339b754cf843624a64a2ef",
          "message": "add tests and fix edge_earliest_time() when the default layer is empty (#1249)",
          "timestamp": "2023-09-07T16:14:19+01:00",
          "tree_id": "ebc88a92378ac262e3ea38d045e2d9b50d6171e4",
          "url": "https://github.com/Pometry/Raphtory/commit/fe1fad786a0a09ae2c339b754cf843624a64a2ef"
        },
        "date": 1694100241949,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 535035,
            "range": "± 1507",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 884721,
            "range": "± 4350",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 948537,
            "range": "± 4520",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 867850,
            "range": "± 52122",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1418799,
            "range": "± 48081",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 79,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 78,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5187,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 33471,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 28416,
            "range": "± 726",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 110,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 78,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5574,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 64,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15176,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 258925,
            "range": "± 102",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 27678,
            "range": "± 269",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 112,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 78,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5101,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 67,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9500,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 158700,
            "range": "± 83",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "25484244+rachchan@users.noreply.github.com",
            "name": "Rachel Chan",
            "username": "rachchan"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "3ca623072b487e3a2793b4a0a41e0f093fb27147",
          "message": "Created new export methods and updated notebooks (#1230)\n\n* created new export methods and updated notebooks\r\n\r\n* docs test passed\r\n\r\n* add tests and update if statements\r\n\r\n* tided up graph conversion\r\n\r\n* Added a load of conversion tests and formatted via black\r\n\r\n* format and added update history to networkx\r\n\r\n* Fully tested to_df\r\n\r\n* Fmt\r\n\r\n* Fixed 3.8\r\n\r\n* Fixed notebook\r\n\r\n* Fixed lucas suggestions\r\n\r\n* fmt\r\n\r\n* fix code coverage path\r\n\r\n* fix coverage 2.0\r\n\r\n* round 3 3 3\r\n\r\n* 4\r\n\r\n---------\r\n\r\nCo-authored-by: miratepuffin <b.a.steer@qmul.ac.uk>",
          "timestamp": "2023-09-07T17:27:04+01:00",
          "tree_id": "4af214c834e7e2b76813e34c147bd501fcedf773",
          "url": "https://github.com/Pometry/Raphtory/commit/3ca623072b487e3a2793b4a0a41e0f093fb27147"
        },
        "date": 1694104682366,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 616927,
            "range": "± 36819",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1035919,
            "range": "± 55376",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1071816,
            "range": "± 3106",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1215518,
            "range": "± 69590",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1985477,
            "range": "± 63048",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 91,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 90,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 6062,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 39891,
            "range": "± 123",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 31813,
            "range": "± 507",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 118,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 88,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 6979,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 62,
            "range": "± 22",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 17837,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 306962,
            "range": "± 141",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 31413,
            "range": "± 879",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 139,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 87,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6523,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 88,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 25,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 11303,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 201567,
            "range": "± 72",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "41898282+github-actions[bot]@users.noreply.github.com",
            "name": "github-actions[bot]",
            "username": "github-actions[bot]"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "dbd543c73a29dd1794e08cf086697db1c6592fbd",
          "message": "Release v0.5.6 (#1252)\n\nchore: Release\r\n\r\nCo-authored-by: Haaroon <Haaroon@users.noreply.github.com>",
          "timestamp": "2023-09-07T18:11:39+01:00",
          "tree_id": "dd8167963202249b26fa5f66322c069c64e8cee8",
          "url": "https://github.com/Pometry/Raphtory/commit/dbd543c73a29dd1794e08cf086697db1c6592fbd"
        },
        "date": 1694107403340,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 593369,
            "range": "± 32384",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 1054039,
            "range": "± 50389",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 1064112,
            "range": "± 55281",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 1262780,
            "range": "± 225076",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1762721,
            "range": "± 152062",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 5,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 92,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 86,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 26,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 26,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5702,
            "range": "± 260",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 38478,
            "range": "± 2318",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 33818,
            "range": "± 842",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 119,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 85,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 7007,
            "range": "± 233",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 78,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 26,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 17434,
            "range": "± 501",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 347939,
            "range": "± 13091",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 33357,
            "range": "± 1314",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 131,
            "range": "± 19",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 89,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 6601,
            "range": "± 127",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 85,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 26,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 11269,
            "range": "± 354",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 215324,
            "range": "± 4214",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "4a3b05b9d58d47af47bc568cedbaaf3c25032d77",
          "message": "Minor API improvements (#1250)\n\n* add tests and fix edge_earliest_time() when the default layer is empty\r\n\r\n* make VID and EID deconstructible\r\n\r\n* make Task apis usable outside of crate\r\n\r\n* make it possible to provide initialised local state for algorithms\r\n\r\n* improve algorithm output api\r\n\r\n* make get for algorithm result behave as expected\r\n\r\n* bump pyo3 to 0.19.2\r\n\r\n* bump serde_with\r\n\r\n* bump display-error-chain\r\n\r\n* add comment about tantivy 0.21 issue",
          "timestamp": "2023-09-09T21:41:31+01:00",
          "tree_id": "a571a05ff9834d72c629ebeaf6d23d169c558d24",
          "url": "https://github.com/Pometry/Raphtory/commit/4a3b05b9d58d47af47bc568cedbaaf3c25032d77"
        },
        "date": 1694292710914,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 506028,
            "range": "± 1250",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 870895,
            "range": "± 7966",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 911492,
            "range": "± 4297",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 923588,
            "range": "± 84820",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1619060,
            "range": "± 97994",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 76,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 74,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 2,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 21,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 4978,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 31916,
            "range": "± 28",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 27305,
            "range": "± 353",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 101,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 74,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5848,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 64,
            "range": "± 17",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 21,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15098,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 260170,
            "range": "± 119",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 26630,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 110,
            "range": "± 14",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 76,
            "range": "± 13",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5592,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 71,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 21,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9679,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 167033,
            "range": "± 58",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "97447091+ljeub-pometry@users.noreply.github.com",
            "name": "ljeub-pometry",
            "username": "ljeub-pometry"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "c4258d840bd2dc11eec70d1f8940852de9205a8d",
          "message": "refactor and clean up the AlgorithmResult struct (#1255)\n\n* refactor and clean up the AlgorithmResult struct\r\n\r\n* cleanup and fix Debug implementation for AlgorithmResult\r\n\r\n* remove unnecessary brackets",
          "timestamp": "2023-09-11T10:55:18+01:00",
          "tree_id": "12d286ee06b3ed9eeebeac8c701812284a74f30e",
          "url": "https://github.com/Pometry/Raphtory/commit/c4258d840bd2dc11eec70d1f8940852de9205a8d"
        },
        "date": 1694426700134,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 545783,
            "range": "± 1784",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 897352,
            "range": "± 7969",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 936740,
            "range": "± 3625",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 811891,
            "range": "± 33451",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1402718,
            "range": "± 38254",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 79,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 79,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 25,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 5207,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 33615,
            "range": "± 100",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 29603,
            "range": "± 655",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 100,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 78,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5668,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 66,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 15196,
            "range": "± 16",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 257698,
            "range": "± 102",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 27736,
            "range": "± 638",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 110,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 79,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5159,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 68,
            "range": "± 10",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 26,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9790,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 156578,
            "range": "± 55",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "b.a.steer@qmul.ac.uk",
            "name": "Ben Steer",
            "username": "miratepuffin"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "ecdcbf5c29367e7edc2dee58a0aff5eb4242f802",
          "message": "Bump Cargo.toml in python back to 0.5.6",
          "timestamp": "2023-09-11T11:29:31+01:00",
          "tree_id": "0d8c67fa3d452523a19e06d8d3907da9693b6c77",
          "url": "https://github.com/Pometry/Raphtory/commit/ecdcbf5c29367e7edc2dee58a0aff5eb4242f802"
        },
        "date": 1694428771396,
        "tool": "cargo",
        "benches": [
          {
            "name": "large/1k fixed edge updates with varying time",
            "value": 500358,
            "range": "± 2063",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and numeric string input",
            "value": 861294,
            "range": "± 1503",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k fixed edge updates with varying time and string input",
            "value": 892890,
            "range": "± 2967",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions",
            "value": 966636,
            "range": "± 74385",
            "unit": "ns/iter"
          },
          {
            "name": "large/1k random edge additions with numeric string input",
            "value": 1647550,
            "range": "± 74597",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_edges",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_existing",
            "value": 76,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_edge_nonexisting",
            "value": 74,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/num_vertices",
            "value": 1,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_existing",
            "value": 22,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_id",
            "value": 4978,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph/max_degree",
            "value": 33689,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_edges",
            "value": 26934,
            "range": "± 235",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_existing",
            "value": 101,
            "range": "± 15",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_edge_nonexisting",
            "value": 73,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/num_vertices",
            "value": 5956,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_existing",
            "value": 70,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_id",
            "value": 14989,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_100/max_degree",
            "value": 255090,
            "range": "± 198",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_edges",
            "value": 27175,
            "range": "± 276",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_existing",
            "value": 98,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_edge_nonexisting",
            "value": 75,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/num_vertices",
            "value": 5479,
            "range": "± 1",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_existing",
            "value": 73,
            "range": "± 11",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/has_vertex_nonexisting",
            "value": 21,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_id",
            "value": 9421,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "lotr_graph_window_10/max_degree",
            "value": 167008,
            "range": "± 80",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}