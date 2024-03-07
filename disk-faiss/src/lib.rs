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
                std::vector<const faiss::InvertedLists*> ivfs;
                std::cout << "reading shards -> " << shards->size() << std::endl;
                size_t ntotal = 0;
                for (unsigned int i = 0; i < num_shards; ++i) {
                    const char * shard = shards->at(i);
                    auto index = faiss::read_index(shard, faiss::IO_FLAG_MMAP);
                    auto ivf = dynamic_cast<faiss::IndexIVF*>(index);
                    assert(ivf);

                    ivfs.push_back(ivf->invlists);
                    ntotal += ivf->ntotal;

                    // ivf->own_invlists = false;
                    // delete ivf;
                }

                auto index_raw = faiss::read_index(index_path);
                auto index = dynamic_cast<faiss::IndexIVF*>(index_raw);
                assert(index);

                if (index->ntotal != 0) {
                    std::exit(1);
                }

                auto il = new faiss::OnDiskInvertedLists(index->nlist, index->code_size, ivfdata);
                il->merge_from(ivfs.data(), ivfs.size());

                index->replace_invlists(il, true);
                index->ntotal = ntotal;

                // auto invlists = new faiss::OnDiskInvertedLists(
                //     index->nlist, index->code_size, ivfdata
                // );
                // std::cout << "----here----" << std::endl;

                // const faiss::InvertedLists **ivfs_data = (const faiss::InvertedLists**) ivfs.data();
                //     std::cout << "---- about to merge lists with size ---- " << ivfs.size() << std::endl;
                // auto ntotal = invlists->merge_from(ivfs_data, ivfs.size()); // TODO: this has a verbose parameter I can use
                // std::cout << "----here----" << std::endl;

                // index->ntotal = ntotal;
                // index->replace_invlists(invlists, true);
                // invlists.this.disown(); ????????????????????????

                faiss::write_index(index, output);

            } catch (const std::exception &e) {
                std::cerr << "standard exception!" << std::endl;
                std::cerr << e.what() << std::endl;
                throw e;
            } catch (...) {
                std::cerr << "unknown exception!" << std::endl;
                throw;
            }
        })
    };
}
