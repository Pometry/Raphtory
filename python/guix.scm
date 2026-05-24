(define-module (raphtory python)
  #:use-module (ice-9 ftw)
  #:use-module (guix packages)
  #:use-module (guix git-download)
  #:use-module (guix build-system copy)
  #:use-module (guix build-system pyproject)
  #:use-module (guix build-system cargo)
  #:use-module (guix gexp)
  #:use-module (guix import crate)
  #:use-module ((guix licenses) #:prefix license:)
  #:use-module (gnu packages check)
  #:use-module (gnu packages cmake)
  #:use-module (gnu packages compression)
  #:use-module (gnu packages databases)
  #:use-module (gnu packages duckdb)
  #:use-module (gnu packages pkg-config)
  #:use-module (gnu packages protobuf)
  #:use-module (gnu packages python-build)
  #:use-module (gnu packages python-check)
  #:use-module (gnu packages python-science)
  #:use-module (gnu packages python-web)
  #:use-module (gnu packages python-xyz)
  #:use-module (gnu packages rust)
  #:use-module (gnu packages rust-apps))

(define %raphtory-root
  (dirname (dirname (current-filename))))

(define-public raphtory-source
  (package
    (name "raphtory-source")
    (version "0.17.0")
    (source
     (origin
       (method git-fetch)
       (uri (git-reference
             (url "https://github.com/Pometry/Raphtory.git")
             (commit "fa6d8d241a68284957a18eb35c7e6d9c4ad59b65")))
       (file-name (git-file-name name version))
       (sha256
        (base32 "010mpqpacrnnnvpq2h46nmxp4gbym559ls11pbimccsaswv6syg0"))))
    (build-system copy-build-system)
    (arguments
     (list #:install-plan #~'(("." "."))))
    (home-page "https://github.com/Pometry/Raphtory")
    (synopsis "Source checkout for Raphtory")
    (description
     "This package provides the Raphtory source tree for dependent Guix
packages.  Keeping it as a named package allows @option{--with-source}
replacement during local development.")
    (license license:gpl3)))

(define-public raphtory-data
  (package
    (name "raphtory-data")
    (version "0")
    (source
     (origin
       (method git-fetch)
       (uri (git-reference
             (url "https://github.com/Raphtory/Data.git")
             (commit "e377ea5c0fe9486902ae911d51cb9f061424a8a6")))
       (file-name (git-file-name name version))
       (sha256
        (base32 "095h9xcxf5x5qwxzr9xagsd4lb4x5pqy4y619z8spxs0q9f4f93c"))))
    (build-system copy-build-system)
    (arguments
     (list #:install-plan #~'(("." "share/raphtory/data"))))
    (home-page "https://github.com/Raphtory/Data")
    (synopsis "Test datasets for Raphtory")
    (description
     "This package provides the Raphtory test datasets (LOTR, Reddit, etc.)
used by the Raphtory test suite and graph loader.")
    (license license:gpl3)))

(define-public python-pyvis
  (package
    (name "python-pyvis")
    (version "0.3.2")
    (source
     (origin
       (method git-fetch)
       (uri (git-reference
             (url "https://github.com/WestHealth/pyvis")
             (commit (string-append "v" version))))
       (file-name (git-file-name name version))
       (sha256
        (base32 "18gp652i4z8r5rxs9mk5h2vx1bc52xlw5wramidb11ilcy9lr3vs"))))
    (build-system pyproject-build-system)
    (arguments '(#:tests? #f))
    (native-inputs (list python-setuptools))
    (propagated-inputs
     (list python-ipython
           python-jinja2
           python-jsonpickle
           python-networkx))
    (home-page "https://github.com/WestHealth/pyvis")
    (synopsis "Python network graph visualization library")
    (description
     "Pyvis is a Python library for visualizing network graphs, built around
the vis.js JavaScript visualization library.")
    (license license:bsd-3)))

(define-public python-raphtory
  (package
    (name "python-raphtory")
    (version "0.17.0")
    (source raphtory-source)
    (build-system pyproject-build-system)
    (arguments
     (list
      #:imported-modules `(,@%cargo-build-system-modules
                           ,@%pyproject-build-system-modules)
      #:modules '(((guix build cargo-build-system) #:prefix cargo:)
                  (guix build pyproject-build-system)
                  (guix build utils))
      #:phases
      #~(modify-phases %standard-phases
          (add-after 'unpack 'relax-maturin-requirement
            (lambda _
              (substitute* "python/pyproject.toml"
                (("maturin>=1\\.8\\.3") "maturin"))))
          (add-after 'relax-maturin-requirement 'prepare-cargo-build-system
            (lambda args
              (substitute "raphtory/src/graph_loader/stable_coins.rs"
                `(("fetch_file\\(zip_str,false,"
                   . ,(lambda (line matches)
                        ""))
                  ("unzip_file\\(zip_str,dir_str\\)"
                   . ,(lambda (line matches) ""))))
              (for-each
               (lambda (phase)
                 (format #t "Running cargo phase: ~a~%" phase)
                 (apply (assoc-ref cargo:%standard-phases phase)
                        #:cargo-target #$(cargo-triplet)
                        args))
               '(prepare-rust-crates
                 unpack-rust-crates
                 configure
                 check-for-pregenerated-files
                 patch-cargo-checksums))))
          (add-after 'prepare-cargo-build-system 'seed-test-data
            (lambda _
              (let* ((data-dir
                      (string-append
                       #$(this-package-native-input "raphtory-data")
                       "/share/raphtory/data"))
                     (tmp-dir (or (getenv "TMPDIR") "/tmp"))
                     (stablecoin-dir "/tmp/stablecoin"))
                (for-each
                 (lambda (file)
                   (symlink (string-append data-dir "/" file)
                            (string-append tmp-dir "/" file)))
                 '("lotr.csv"
                   "lotr_properties.csv"
                   "lotr_test.csv"
                   "reddit-title-test.tsv"
                   "soc-redditHyperlinks-title.tsv"
                   "sx-superuser.txt.gz"))
                (symlink (string-append data-dir "/lotr_test.csv")
                         (string-append tmp-dir "/lotr2.csv"))
                (symlink (string-append data-dir
                                         "/soc-redditHyperlinks-title.tsv")
                         (string-append tmp-dir "/reddit-title.tsv"))
                (mkdir-p stablecoin-dir)
                (symlink (string-append data-dir "/token_transfers.csv")
                         (string-append stablecoin-dir
                                        "/token_transfers.csv")))))
          (add-after 'seed-test-data 'set-cargo-profile-overrides
            (lambda _
              (setenv "CARGO_PROFILE_RELEASE_DEBUG" "0")
              (setenv "CARGO_PROFILE_RELEASE_STRIP" "symbols")))
          (add-after 'set-cargo-profile-overrides 'enter-python-package
            (lambda _
              (chdir "python"))))))
    (inputs
     (append
      (list bzip2
            lz4
            xz
            `(,zstd "lib")
            `(,zstd "static"))
      (cargo-inputs-from-lockfile
       (string-append %raphtory-root "/Cargo.lock"))))
    (native-inputs
     (append
      (list cmake
            maturin
            pkg-config
            protobuf
            python-duckdb
            python-pytest
            python-pytest-benchmark
            python-pyvis
            python-requests
            raphtory-data
            rust-1.91
            `(,rust-1.91 "cargo"))
      (or (and=> (%current-target-system)
                 (compose list make-rust-sysroot))
          '())))
    (propagated-inputs
     (list python-ipywidgets
           python-networkx
           python-numpy
           python-pyjwt
           python-matplotlib
           python-polars
           python-pandas
           python-pyarrow
           python-seaborn))
    (home-page "https://github.com/Pometry/Raphtory")
    (synopsis "Python bindings for Raphtory")
    (description
     "Raphtory is a temporal graph analytics engine.  This package provides
the Python library backed by Raphtory's Rust implementation through PyO3 and
maturin.")
    (license license:gpl3)))

python-raphtory
