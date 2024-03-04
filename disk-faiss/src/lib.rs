#![recursion_limit = "512"]
// #![cfg_attr(not(test), allow(dead_code, unused_imports))]
// #![allow(unused)]

// #[macro_use]
// extern crate cpp;

use cpp::cpp;

cpp! {{
    #include <stdio.h>
    #include <faiss/IndexIVFFlat.h>
    #include <faiss/invlists/OnDiskInvertedLists.h>
    #include <faiss/index_factory.h>
    #include <faiss/MetaIndexes.h>
    #include <faiss/index_io.h>
}}

pub fn merge_ondisk(index: &str, shards: Vec<&str>, ivfdata: &str, output: &str) {
    let index_path = std::ffi::CString::new(index).unwrap();
    let index_path = index.as_ptr();

    let shards: Vec<_> = shards
        .iter()
        .map(|shard| std::ffi::CString::new(*shard).unwrap())
        .collect();
    let shards: Vec<_> = shards.iter().map(|shard| shard.as_ptr()).collect();
    let shards = &shards;

    let ivfdata = std::ffi::CString::new(ivfdata).unwrap();
    let ivfdata = ivfdata.as_ptr();

    let output = std::ffi::CString::new(output).unwrap();
    let output = output.as_ptr();

    unsafe {
        cpp!([index_path as "const char *", shards as "std::vector<const char *> *", ivfdata as "const char *", output as "const char *"] {

            // try {
                faiss::IndexIVFFlat* index = (faiss::IndexIVFFlat*) faiss::read_index(index_path, 0); // TODO: review: 0????????????????????
                // auto index_ref = dynamic_cast<faiss::IndexIDMap*>(index);
                // return *index_ref ;

                std::vector<faiss::IndexIVFFlat*> ivfs;
                for (const auto& shard : *shards) {
                    faiss::IndexIVFFlat* ivf = (faiss::IndexIVFFlat*) faiss::read_index(shard, faiss::IO_FLAG_MMAP);
                    ivfs.push_back(ivf);
                }

                if (index->ntotal != 0) {
                    std::exit(1);
                }

                auto invlists = faiss::OnDiskInvertedLists(
                    index->nlist, index->code_size, ivfdata
                );

                // auto ivf_vector = faiss::InvertedListsPtrVector();
                // for (const auto& ivf : ivfs) {
                //     ivf_vector.push_back(ivf);
                // }

                const faiss::InvertedLists **ivfs_data = (const faiss::InvertedLists**) ivfs.data();
                auto ntotal = invlists.merge_from(ivfs_data, ivfs.size()); // TODO: this has a verbose parameter I can use

                index->ntotal = ntotal;
                index->replace_invlists(&invlists, true);
                // invlists.this.disown(); ????????????????????????

                faiss::write_index(index, output);
            // } catch (const std::exception &e) {
            //     std::cerr << "cpp exception";
            //     std::cerr << e.what();
            // } catch {
            //     std::cerr << "unknown exception";
            // }
        })
    };
}
