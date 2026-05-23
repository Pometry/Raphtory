(define-module (raphtory packages)
  #:use-module (guix packages)
  #:use-module (guix git-download)
  #:use-module (guix build-system cargo)
  #:use-module (guix gexp)
  #:use-module ((guix licenses) #:prefix license:)
  #:use-module (guix import crate)
  #:use-module (gnu packages rust)
  #:use-module (gnu packages cmake)
  #:use-module (gnu packages pkg-config)
  #:use-module (gnu packages protobuf)
  #:use-module (gnu packages compression))

(define-public raphtory-graphql
  (package
    (name "raphtory-graphql")
    (version "0.17.0")
    (source
     (origin
       (method git-fetch)
       (uri (git-reference
             (url "https://github.com/Raphtory/Raphtory")
             (commit "fa6d8d241a68284957a18eb35c7e6d9c4ad59b65")))
       (file-name (string-append name "-" version "-checkout"))
       (sha256
        (base32 "010mpqpacrnnnvpq2h46nmxp4gbym559ls11pbimccsaswv6syg0"))))
    (build-system cargo-build-system)
    (arguments
     (list
      #:install-source? #f
      #:cargo-install-paths ''("raphtory-graphql")
      #:tests? #f
      #:phases
      #~(modify-phases %standard-phases
          (add-before 'build 'set-raphtory-ui-path
            (lambda _
              (setenv "RAPHTORY_UI_INDEX_PATH"
                      (string-append (getcwd) "/raphtory-graphql/resources")))))))
    (native-inputs (list cmake pkg-config protobuf))
    (inputs
     (append
      (list rust-1.91
            `(,rust-1.91 "cargo")
            bzip2 lz4 xz
            `(,zstd "lib") `(,zstd "static"))
      (cargo-inputs-from-lockfile "Cargo.lock")))
    (home-page "https://raphtory.com")
    (synopsis "Temporal graph analytics engine with a GraphQL server")
    (description
     "Raphtory is a Rust-based temporal graph analytics engine.  This package
provides the GraphQL server, which exposes graph operations including PageRank,
shortest path, and other algorithms over a GraphQL API.")
    (license license:gpl3)))

raphtory-graphql
