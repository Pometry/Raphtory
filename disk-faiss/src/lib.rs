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
    let num_shards: u32 = shards.len() as u32;

    let ivfdata = std::ffi::CString::new(ivfdata).unwrap();
    let ivfdata = ivfdata.as_ptr();

    let output = std::ffi::CString::new(output).unwrap();
    let output = output.as_ptr();

    unsafe {
        cpp!([index_path as "const char *", shards as "std::vector<const char *> *", num_shards as "uint32_t", ivfdata as "const char *", output as "const char *"] {

            try {
            std::cout << "----here----" << std::endl;
            std::vector<faiss::IndexIVFFlat*> ivfs;
            std::cout << "reading shards -> " << shards->size() << std::endl;
            for (unsigned int i = 0; i < num_shards; ++i) {
                // std::cout << "reading " << shard << std::endl;
                const char * shard = shards->at(i);
                faiss::IndexIVFFlat* ivf = (faiss::IndexIVFFlat*) faiss::read_index(shard, faiss::IO_FLAG_MMAP);
                std::cout << "success reading" << std::endl;
                ivfs.push_back(ivf);
                std::cout << "success adding it" << std::endl;

                ivf->own_invlists = false;
                delete ivf;
            }

            std::cout << "---- after reading shards ----" << std::endl;
            faiss::IndexIVFFlat* index = (faiss::IndexIVFFlat*) faiss::read_index(index_path); // TODO: review: 0 as second parameter????????????????????

            if (index->ntotal != 0) {
                std::exit(1);
            }
            std::cout << "nlist: " << index->nlist << std::endl;
            std::cout << "code_size: " << index->code_size << std::endl;
            std::cout << "---- about to call on disk inverted lists ----" << std::endl;
            auto invlists = new faiss::OnDiskInvertedLists(
                index->nlist, index->code_size, ivfdata
            );
            std::cout << "----here----" << std::endl;

            // auto ivf_vector = faiss::InvertedListsPtrVector();
            // for (const auto& ivf : ivfs) {
            //     ivf_vector.push_back(ivf);
            // }

            const faiss::InvertedLists **ivfs_data = (const faiss::InvertedLists**) ivfs.data();
            auto ntotal = invlists->merge_from(ivfs_data, ivfs.size()); // TODO: this has a verbose parameter I can use
            std::cout << "--here--";

            index->ntotal = ntotal;
            index->replace_invlists(invlists, true);
            std::cout << "--here--";
            // invlists.this.disown(); ????????????????????????

            faiss::write_index(index, output);

            } catch (const std::exception &e) {
                std::cerr << "cpp exception" << std::endl;
                std::cerr << e.what() << std::endl;
                throw e;
            }
        })
    };
}
