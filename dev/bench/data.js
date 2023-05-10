window.BENCHMARK_DATA = {
  "lastUpdate": 1683708220702,
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
      }
    ]
  }
}