window.BENCHMARK_DATA = {
  "lastUpdate": 1688660451684,
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
      }
    ]
  }
}