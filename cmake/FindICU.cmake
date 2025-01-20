if(NOT TARGET icu)
    message(STATUS "ICU target not found, checking for local sources...")

    set(ICU_ARCHIVE_URL "https://github.com/unicode-org/icu/archive/refs/tags/release-75-1.tar.gz")
    set(ICU_ARCHIVE "${CMAKE_SOURCE_DIR}/3rdparty/icu-75-1.tar.gz")
    set(ICU_EXTRACT_DIR "${CMAKE_SOURCE_DIR}/3rdparty")
    set(ICU_SOURCE_DIR "${CMAKE_SOURCE_DIR}/3rdparty/icu/icu4c/source")
    set(ICUDATA_SOURCE_DIR "${ICU_SOURCE_DIR}/data/out/tmp")

    # Check if ICU source directory already exists
    if (NOT EXISTS "${ICU_EXTRACT_DIR}/icu")
        message(STATUS "ICU source directory not found, downloading and building ICU...")

        # Download ICU archive
        if (NOT EXISTS "${ICU_ARCHIVE}")
            message(STATUS "Downloading ICU archive from GitHub...")
            execute_process(
                COMMAND wget -q "${ICU_ARCHIVE_URL}" -O "${ICU_ARCHIVE}"
                RESULT_VARIABLE download_result
            )

            # Check if the download was successful
            if(NOT download_result EQUAL 0)
                message(FATAL_ERROR "Failed to download ICU archive.")
            else()
                message(STATUS "Download complete: ${ICU_ARCHIVE}")
            endif()
        else()
            message(STATUS "ICU archive already exists: ${ICU_ARCHIVE}")
        endif()

        # Extract ICU archive
        message(STATUS "Extracting ICU archive to ${ICU_EXTRACT_DIR}...")
        execute_process(
            COMMAND ${CMAKE_COMMAND} -E tar xzf "${ICU_ARCHIVE}"
            WORKING_DIRECTORY "${ICU_EXTRACT_DIR}"
            RESULT_VARIABLE extract_result
        )

       # Check if the extraction was successful
        if(NOT extract_result EQUAL 0)
            message(FATAL_ERROR "Failed to extract ICU archive: ${ICU_ARCHIVE}")
        else()
            message(STATUS "Extraction complete.")
        endif()

        # Rename the extracted directory
        set(ICU_EXTRACTED_DIR "${ICU_EXTRACT_DIR}/icu-release-75-1")
        set(ICU_RENAMED_DIR "${ICU_EXTRACT_DIR}/icu")

        # Check if the target directory already exists
        if (EXISTS "${ICU_RENAMED_DIR}")
            message(STATUS "Removing existing directory: ${ICU_RENAMED_DIR}...")
            file(REMOVE_RECURSE "${ICU_RENAMED_DIR}")
        endif()

        if (EXISTS "${ICU_EXTRACTED_DIR}")
            message(STATUS "Renaming extracted directory from ${ICU_EXTRACTED_DIR} to ${ICU_RENAMED_DIR}...")
            execute_process(
                COMMAND mv "${ICU_EXTRACTED_DIR}" "${ICU_RENAMED_DIR}"
                RESULT_VARIABLE rename_result
            )

            # Check if the renaming was successful
            if(NOT rename_result EQUAL 0)
                message(FATAL_ERROR "Failed to rename directory: ${ICU_EXTRACTED_DIR}")
            else()
                message(STATUS "Directory renamed to: ${ICU_RENAMED_DIR}")
            endif()
        else()
            message(FATAL_ERROR "Extracted directory not found: ${ICU_EXTRACTED_DIR}")
        endif()

        # Remove the downloaded archive
        message(STATUS "Removing ICU archive: ${ICU_ARCHIVE}...")
        file(REMOVE "${ICU_ARCHIVE}")
        message(STATUS "ICU archive removed.")

        # Configure ICU
        message(STATUS "Configuring ICU...")
        execute_process(
            COMMAND ./configure
            WORKING_DIRECTORY "${ICU_SOURCE_DIR}"
            RESULT_VARIABLE configure_result
        )

        # Check if the configuration was successful
        if(NOT configure_result EQUAL 0)
            message(FATAL_ERROR "Failed to configure ICU.")
        else()
            message(STATUS "ICU configuration complete.")
        endif()

        # Build ICU
        message(STATUS "Building ICU...")
        execute_process(
            COMMAND make -j${CMAKE_BUILD_PARALLEL_LEVEL}
            WORKING_DIRECTORY "${ICU_SOURCE_DIR}"
            RESULT_VARIABLE build_result
        )

        # Check if the build was successful
        if(NOT build_result EQUAL 0)
            message(FATAL_ERROR "Failed to build ICU.")
        else()
            message(STATUS "ICU build complete.")
        endif()
    else()
        message(STATUS "ICU source directory already exists, skipping.")
    endif()

    # # Check if ICU source directory already exists
    # if(NOT EXISTS "${ICU_SOURCE_DIR}")
    #     message(STATUS "ICU source directory not found, extracting ICU archive...")

    #     if(EXISTS "${ICU_EXTRACT_DIR}/icu")
    #         message(STATUS "Removing existing ICU directory...")
    #         file(REMOVE_RECURSE "${ICU_EXTRACT_DIR}/icu")
    #     endif()

    #     message(STATUS "Extracting ICU archive to ${ICU_EXTRACT_DIR}...")
    #     execute_process(
    #         COMMAND ${CMAKE_COMMAND} -E tar xzf "${ICU_ARCHIVE}"
    #         WORKING_DIRECTORY "${ICU_EXTRACT_DIR}"
    #         RESULT_VARIABLE extract_result
    #     )

    #     if(NOT extract_result EQUAL 0)
    #         message(FATAL_ERROR "Failed to extract ICU archive: ${ICU_ARCHIVE}")
    #     endif()
    # else()
    #     message(STATUS "ICU source directory already exists, skipping extraction.")
    # endif()

    # These lists of sources were generated from build log of the original ICU build system (configure + make).

    set(ICUUC_SOURCES
    "${ICU_SOURCE_DIR}/common/appendable.cpp"
    "${ICU_SOURCE_DIR}/common/bmpset.cpp"
    "${ICU_SOURCE_DIR}/common/brkeng.cpp"
    "${ICU_SOURCE_DIR}/common/brkiter.cpp"
    "${ICU_SOURCE_DIR}/common/bytesinkutil.cpp"
    "${ICU_SOURCE_DIR}/common/bytestream.cpp"
    "${ICU_SOURCE_DIR}/common/bytestrie.cpp"
    "${ICU_SOURCE_DIR}/common/bytestriebuilder.cpp"
    "${ICU_SOURCE_DIR}/common/bytestrieiterator.cpp"
    "${ICU_SOURCE_DIR}/common/caniter.cpp"
    "${ICU_SOURCE_DIR}/common/characterproperties.cpp"
    "${ICU_SOURCE_DIR}/common/chariter.cpp"
    "${ICU_SOURCE_DIR}/common/charstr.cpp"
    "${ICU_SOURCE_DIR}/common/cmemory.cpp"
    "${ICU_SOURCE_DIR}/common/cstr.cpp"
    "${ICU_SOURCE_DIR}/common/cstring.cpp"
    "${ICU_SOURCE_DIR}/common/cwchar.cpp"
    "${ICU_SOURCE_DIR}/common/dictbe.cpp"
    "${ICU_SOURCE_DIR}/common/dictionarydata.cpp"
    "${ICU_SOURCE_DIR}/common/dtintrv.cpp"
    "${ICU_SOURCE_DIR}/common/edits.cpp"
    "${ICU_SOURCE_DIR}/common/emojiprops.cpp"
    "${ICU_SOURCE_DIR}/common/errorcode.cpp"
    "${ICU_SOURCE_DIR}/common/filteredbrk.cpp"
    "${ICU_SOURCE_DIR}/common/filterednormalizer2.cpp"
    "${ICU_SOURCE_DIR}/common/icudataver.cpp"
    "${ICU_SOURCE_DIR}/common/icuplug.cpp"
    "${ICU_SOURCE_DIR}/common/loadednormalizer2impl.cpp"
    "${ICU_SOURCE_DIR}/common/localebuilder.cpp"
    "${ICU_SOURCE_DIR}/common/localematcher.cpp"
    "${ICU_SOURCE_DIR}/common/localeprioritylist.cpp"
    "${ICU_SOURCE_DIR}/common/locavailable.cpp"
    "${ICU_SOURCE_DIR}/common/locbased.cpp"
    "${ICU_SOURCE_DIR}/common/locdispnames.cpp"
    "${ICU_SOURCE_DIR}/common/locdistance.cpp"
    "${ICU_SOURCE_DIR}/common/locdspnm.cpp"
    "${ICU_SOURCE_DIR}/common/locid.cpp"
    "${ICU_SOURCE_DIR}/common/loclikely.cpp"
    "${ICU_SOURCE_DIR}/common/loclikelysubtags.cpp"
    "${ICU_SOURCE_DIR}/common/locmap.cpp"
    "${ICU_SOURCE_DIR}/common/locresdata.cpp"
    "${ICU_SOURCE_DIR}/common/locutil.cpp"
    "${ICU_SOURCE_DIR}/common/lsr.cpp"
    "${ICU_SOURCE_DIR}/common/lstmbe.cpp"
    "${ICU_SOURCE_DIR}/common/messagepattern.cpp"
    "${ICU_SOURCE_DIR}/common/mlbe.cpp"
    "${ICU_SOURCE_DIR}/common/normalizer2.cpp"
    "${ICU_SOURCE_DIR}/common/normalizer2impl.cpp"
    "${ICU_SOURCE_DIR}/common/normlzr.cpp"
    "${ICU_SOURCE_DIR}/common/parsepos.cpp"
    "${ICU_SOURCE_DIR}/common/patternprops.cpp"
    "${ICU_SOURCE_DIR}/common/pluralmap.cpp"
    "${ICU_SOURCE_DIR}/common/propname.cpp"
    "${ICU_SOURCE_DIR}/common/propsvec.cpp"
    "${ICU_SOURCE_DIR}/common/punycode.cpp"
    "${ICU_SOURCE_DIR}/common/putil.cpp"
    "${ICU_SOURCE_DIR}/common/rbbi.cpp"
    "${ICU_SOURCE_DIR}/common/rbbi_cache.cpp"
    "${ICU_SOURCE_DIR}/common/rbbidata.cpp"
    "${ICU_SOURCE_DIR}/common/rbbinode.cpp"
    "${ICU_SOURCE_DIR}/common/rbbirb.cpp"
    "${ICU_SOURCE_DIR}/common/rbbiscan.cpp"
    "${ICU_SOURCE_DIR}/common/rbbisetb.cpp"
    "${ICU_SOURCE_DIR}/common/rbbistbl.cpp"
    "${ICU_SOURCE_DIR}/common/rbbitblb.cpp"
    "${ICU_SOURCE_DIR}/common/resbund.cpp"
    "${ICU_SOURCE_DIR}/common/resbund_cnv.cpp"
    "${ICU_SOURCE_DIR}/common/resource.cpp"
    "${ICU_SOURCE_DIR}/common/restrace.cpp"
    "${ICU_SOURCE_DIR}/common/ruleiter.cpp"
    "${ICU_SOURCE_DIR}/common/schriter.cpp"
    "${ICU_SOURCE_DIR}/common/serv.cpp"
    "${ICU_SOURCE_DIR}/common/servlk.cpp"
    "${ICU_SOURCE_DIR}/common/servlkf.cpp"
    "${ICU_SOURCE_DIR}/common/servls.cpp"
    "${ICU_SOURCE_DIR}/common/servnotf.cpp"
    "${ICU_SOURCE_DIR}/common/servrbf.cpp"
    "${ICU_SOURCE_DIR}/common/servslkf.cpp"
    "${ICU_SOURCE_DIR}/common/sharedobject.cpp"
    "${ICU_SOURCE_DIR}/common/simpleformatter.cpp"
    "${ICU_SOURCE_DIR}/common/static_unicode_sets.cpp"
    "${ICU_SOURCE_DIR}/common/stringpiece.cpp"
    "${ICU_SOURCE_DIR}/common/stringtriebuilder.cpp"
    "${ICU_SOURCE_DIR}/common/uarrsort.cpp"
    "${ICU_SOURCE_DIR}/common/ubidi.cpp"
    "${ICU_SOURCE_DIR}/common/ubidi_props.cpp"
    "${ICU_SOURCE_DIR}/common/ubidiln.cpp"
    "${ICU_SOURCE_DIR}/common/ubiditransform.cpp"
    "${ICU_SOURCE_DIR}/common/ubidiwrt.cpp"
    "${ICU_SOURCE_DIR}/common/ubrk.cpp"
    "${ICU_SOURCE_DIR}/common/ucase.cpp"
    "${ICU_SOURCE_DIR}/common/ucasemap.cpp"
    "${ICU_SOURCE_DIR}/common/ucasemap_titlecase_brkiter.cpp"
    "${ICU_SOURCE_DIR}/common/ucat.cpp"
    "${ICU_SOURCE_DIR}/common/uchar.cpp"
    "${ICU_SOURCE_DIR}/common/ucharstrie.cpp"
    "${ICU_SOURCE_DIR}/common/ucharstriebuilder.cpp"
    "${ICU_SOURCE_DIR}/common/ucharstrieiterator.cpp"
    "${ICU_SOURCE_DIR}/common/uchriter.cpp"
    "${ICU_SOURCE_DIR}/common/ucln_cmn.cpp"
    "${ICU_SOURCE_DIR}/common/ucmndata.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv2022.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_bld.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_cb.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_cnv.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_ct.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_err.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_ext.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_io.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_lmb.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_set.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_u16.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_u32.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_u7.cpp"
    "${ICU_SOURCE_DIR}/common/ucnv_u8.cpp"
    "${ICU_SOURCE_DIR}/common/ucnvbocu.cpp"
    "${ICU_SOURCE_DIR}/common/ucnvdisp.cpp"
    "${ICU_SOURCE_DIR}/common/ucnvhz.cpp"
    "${ICU_SOURCE_DIR}/common/ucnvisci.cpp"
    "${ICU_SOURCE_DIR}/common/ucnvlat1.cpp"
    "${ICU_SOURCE_DIR}/common/ucnvmbcs.cpp"
    "${ICU_SOURCE_DIR}/common/ucnvscsu.cpp"
    "${ICU_SOURCE_DIR}/common/ucnvsel.cpp"
    "${ICU_SOURCE_DIR}/common/ucol_swp.cpp"
    "${ICU_SOURCE_DIR}/common/ucptrie.cpp"
    "${ICU_SOURCE_DIR}/common/ucurr.cpp"
    "${ICU_SOURCE_DIR}/common/udata.cpp"
    "${ICU_SOURCE_DIR}/common/udatamem.cpp"
    "${ICU_SOURCE_DIR}/common/udataswp.cpp"
    "${ICU_SOURCE_DIR}/common/uenum.cpp"
    "${ICU_SOURCE_DIR}/common/uhash.cpp"
    "${ICU_SOURCE_DIR}/common/uhash_us.cpp"
    "${ICU_SOURCE_DIR}/common/uidna.cpp"
    "${ICU_SOURCE_DIR}/common/uinit.cpp"
    "${ICU_SOURCE_DIR}/common/uinvchar.cpp"
    "${ICU_SOURCE_DIR}/common/uiter.cpp"
    "${ICU_SOURCE_DIR}/common/ulist.cpp"
    "${ICU_SOURCE_DIR}/common/uloc.cpp"
    "${ICU_SOURCE_DIR}/common/uloc_keytype.cpp"
    "${ICU_SOURCE_DIR}/common/uloc_tag.cpp"
    "${ICU_SOURCE_DIR}/common/ulocale.cpp"
    "${ICU_SOURCE_DIR}/common/ulocbuilder.cpp"
    "${ICU_SOURCE_DIR}/common/umapfile.cpp"
    "${ICU_SOURCE_DIR}/common/umath.cpp"
    "${ICU_SOURCE_DIR}/common/umutablecptrie.cpp"
    "${ICU_SOURCE_DIR}/common/umutex.cpp"
    "${ICU_SOURCE_DIR}/common/unames.cpp"
    "${ICU_SOURCE_DIR}/common/unifiedcache.cpp"
    "${ICU_SOURCE_DIR}/common/unifilt.cpp"
    "${ICU_SOURCE_DIR}/common/unifunct.cpp"
    "${ICU_SOURCE_DIR}/common/uniset.cpp"
    "${ICU_SOURCE_DIR}/common/uniset_closure.cpp"
    "${ICU_SOURCE_DIR}/common/uniset_props.cpp"
    "${ICU_SOURCE_DIR}/common/unisetspan.cpp"
    "${ICU_SOURCE_DIR}/common/unistr.cpp"
    "${ICU_SOURCE_DIR}/common/unistr_case.cpp"
    "${ICU_SOURCE_DIR}/common/unistr_case_locale.cpp"
    "${ICU_SOURCE_DIR}/common/unistr_cnv.cpp"
    "${ICU_SOURCE_DIR}/common/unistr_props.cpp"
    "${ICU_SOURCE_DIR}/common/unistr_titlecase_brkiter.cpp"
    "${ICU_SOURCE_DIR}/common/unorm.cpp"
    "${ICU_SOURCE_DIR}/common/unormcmp.cpp"
    "${ICU_SOURCE_DIR}/common/uobject.cpp"
    "${ICU_SOURCE_DIR}/common/uprops.cpp"
    "${ICU_SOURCE_DIR}/common/ures_cnv.cpp"
    "${ICU_SOURCE_DIR}/common/uresbund.cpp"
    "${ICU_SOURCE_DIR}/common/uresdata.cpp"
    "${ICU_SOURCE_DIR}/common/usc_impl.cpp"
    "${ICU_SOURCE_DIR}/common/uscript.cpp"
    "${ICU_SOURCE_DIR}/common/uscript_props.cpp"
    "${ICU_SOURCE_DIR}/common/uset.cpp"
    "${ICU_SOURCE_DIR}/common/uset_props.cpp"
    "${ICU_SOURCE_DIR}/common/usetiter.cpp"
    "${ICU_SOURCE_DIR}/common/ushape.cpp"
    "${ICU_SOURCE_DIR}/common/usprep.cpp"
    "${ICU_SOURCE_DIR}/common/ustack.cpp"
    "${ICU_SOURCE_DIR}/common/ustr_cnv.cpp"
    "${ICU_SOURCE_DIR}/common/ustr_titlecase_brkiter.cpp"
    "${ICU_SOURCE_DIR}/common/ustr_wcs.cpp"
    "${ICU_SOURCE_DIR}/common/ustrcase.cpp"
    "${ICU_SOURCE_DIR}/common/ustrcase_locale.cpp"
    "${ICU_SOURCE_DIR}/common/ustrenum.cpp"
    "${ICU_SOURCE_DIR}/common/ustrfmt.cpp"
    "${ICU_SOURCE_DIR}/common/ustring.cpp"
    "${ICU_SOURCE_DIR}/common/ustrtrns.cpp"
    "${ICU_SOURCE_DIR}/common/utext.cpp"
    "${ICU_SOURCE_DIR}/common/utf_impl.cpp"
    "${ICU_SOURCE_DIR}/common/util.cpp"
    "${ICU_SOURCE_DIR}/common/util_props.cpp"
    "${ICU_SOURCE_DIR}/common/utrace.cpp"
    "${ICU_SOURCE_DIR}/common/utrie.cpp"
    "${ICU_SOURCE_DIR}/common/utrie2.cpp"
    "${ICU_SOURCE_DIR}/common/utrie2_builder.cpp"
    "${ICU_SOURCE_DIR}/common/utrie_swap.cpp"
    "${ICU_SOURCE_DIR}/common/uts46.cpp"
    "${ICU_SOURCE_DIR}/common/utypes.cpp"
    "${ICU_SOURCE_DIR}/common/uvector.cpp"
    "${ICU_SOURCE_DIR}/common/uvectr32.cpp"
    "${ICU_SOURCE_DIR}/common/uvectr64.cpp"
    "${ICU_SOURCE_DIR}/common/wintz.cpp")

    set(ICUI18N_SOURCES
    "${ICU_SOURCE_DIR}/i18n/alphaindex.cpp"
    "${ICU_SOURCE_DIR}/i18n/anytrans.cpp"
    "${ICU_SOURCE_DIR}/i18n/astro.cpp"
    "${ICU_SOURCE_DIR}/i18n/basictz.cpp"
    "${ICU_SOURCE_DIR}/i18n/bocsu.cpp"
    "${ICU_SOURCE_DIR}/i18n/brktrans.cpp"
    "${ICU_SOURCE_DIR}/i18n/buddhcal.cpp"
    "${ICU_SOURCE_DIR}/i18n/calendar.cpp"
    "${ICU_SOURCE_DIR}/i18n/casetrn.cpp"
    "${ICU_SOURCE_DIR}/i18n/cecal.cpp"
    "${ICU_SOURCE_DIR}/i18n/chnsecal.cpp"
    "${ICU_SOURCE_DIR}/i18n/choicfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/coleitr.cpp"
    "${ICU_SOURCE_DIR}/i18n/coll.cpp"
    "${ICU_SOURCE_DIR}/i18n/collation.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationbuilder.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationcompare.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationdata.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationdatabuilder.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationdatareader.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationdatawriter.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationfastlatin.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationfastlatinbuilder.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationfcd.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationiterator.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationkeys.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationroot.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationrootelements.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationruleparser.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationsets.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationsettings.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationtailoring.cpp"
    "${ICU_SOURCE_DIR}/i18n/collationweights.cpp"
    "${ICU_SOURCE_DIR}/i18n/compactdecimalformat.cpp"
    "${ICU_SOURCE_DIR}/i18n/coptccal.cpp"
    "${ICU_SOURCE_DIR}/i18n/cpdtrans.cpp"
    "${ICU_SOURCE_DIR}/i18n/csdetect.cpp"
    "${ICU_SOURCE_DIR}/i18n/csmatch.cpp"
    "${ICU_SOURCE_DIR}/i18n/csr2022.cpp"
    "${ICU_SOURCE_DIR}/i18n/csrecog.cpp"
    "${ICU_SOURCE_DIR}/i18n/csrmbcs.cpp"
    "${ICU_SOURCE_DIR}/i18n/csrsbcs.cpp"
    "${ICU_SOURCE_DIR}/i18n/csrucode.cpp"
    "${ICU_SOURCE_DIR}/i18n/csrutf8.cpp"
    "${ICU_SOURCE_DIR}/i18n/curramt.cpp"
    "${ICU_SOURCE_DIR}/i18n/currfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/currpinf.cpp"
    "${ICU_SOURCE_DIR}/i18n/currunit.cpp"
    "${ICU_SOURCE_DIR}/i18n/dangical.cpp"
    "${ICU_SOURCE_DIR}/i18n/datefmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/dayperiodrules.cpp"
    "${ICU_SOURCE_DIR}/i18n/dcfmtsym.cpp"
    "${ICU_SOURCE_DIR}/i18n/decContext.cpp"
    "${ICU_SOURCE_DIR}/i18n/decNumber.cpp"
    "${ICU_SOURCE_DIR}/i18n/decimfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/displayoptions.cpp"
    "${ICU_SOURCE_DIR}/i18n/double-conversion-bignum-dtoa.cpp"
    "${ICU_SOURCE_DIR}/i18n/double-conversion-bignum.cpp"
    "${ICU_SOURCE_DIR}/i18n/double-conversion-cached-powers.cpp"
    "${ICU_SOURCE_DIR}/i18n/double-conversion-double-to-string.cpp"
    "${ICU_SOURCE_DIR}/i18n/double-conversion-fast-dtoa.cpp"
    "${ICU_SOURCE_DIR}/i18n/double-conversion-string-to-double.cpp"
    "${ICU_SOURCE_DIR}/i18n/double-conversion-strtod.cpp"
    "${ICU_SOURCE_DIR}/i18n/dtfmtsym.cpp"
    "${ICU_SOURCE_DIR}/i18n/dtitvfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/dtitvinf.cpp"
    "${ICU_SOURCE_DIR}/i18n/dtptngen.cpp"
    "${ICU_SOURCE_DIR}/i18n/dtrule.cpp"
    "${ICU_SOURCE_DIR}/i18n/erarules.cpp"
    "${ICU_SOURCE_DIR}/i18n/esctrn.cpp"
    "${ICU_SOURCE_DIR}/i18n/ethpccal.cpp"
    "${ICU_SOURCE_DIR}/i18n/fmtable.cpp"
    "${ICU_SOURCE_DIR}/i18n/fmtable_cnv.cpp"
    "${ICU_SOURCE_DIR}/i18n/format.cpp"
    "${ICU_SOURCE_DIR}/i18n/formatted_string_builder.cpp"
    "${ICU_SOURCE_DIR}/i18n/formattedval_iterimpl.cpp"
    "${ICU_SOURCE_DIR}/i18n/formattedval_sbimpl.cpp"
    "${ICU_SOURCE_DIR}/i18n/formattedvalue.cpp"
    "${ICU_SOURCE_DIR}/i18n/fphdlimp.cpp"
    "${ICU_SOURCE_DIR}/i18n/fpositer.cpp"
    "${ICU_SOURCE_DIR}/i18n/funcrepl.cpp"
    "${ICU_SOURCE_DIR}/i18n/gender.cpp"
    "${ICU_SOURCE_DIR}/i18n/gregocal.cpp"
    "${ICU_SOURCE_DIR}/i18n/gregoimp.cpp"
    "${ICU_SOURCE_DIR}/i18n/hebrwcal.cpp"
    "${ICU_SOURCE_DIR}/i18n/indiancal.cpp"
    "${ICU_SOURCE_DIR}/i18n/inputext.cpp"
    "${ICU_SOURCE_DIR}/i18n/islamcal.cpp"
    "${ICU_SOURCE_DIR}/i18n/iso8601cal.cpp"
    "${ICU_SOURCE_DIR}/i18n/japancal.cpp"
    "${ICU_SOURCE_DIR}/i18n/listformatter.cpp"
    "${ICU_SOURCE_DIR}/i18n/measfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/measunit.cpp"
    "${ICU_SOURCE_DIR}/i18n/measunit_extra.cpp"
    "${ICU_SOURCE_DIR}/i18n/measure.cpp"
    "${ICU_SOURCE_DIR}/i18n/messageformat2.cpp"
    "${ICU_SOURCE_DIR}/i18n/messageformat2_arguments.cpp"
    "${ICU_SOURCE_DIR}/i18n/messageformat2_checker.cpp"
    "${ICU_SOURCE_DIR}/i18n/messageformat2_data_model.cpp"
    "${ICU_SOURCE_DIR}/i18n/messageformat2_errors.cpp"
    "${ICU_SOURCE_DIR}/i18n/messageformat2_evaluation.cpp"
    "${ICU_SOURCE_DIR}/i18n/messageformat2_formattable.cpp"
    "${ICU_SOURCE_DIR}/i18n/messageformat2_formatter.cpp"
    "${ICU_SOURCE_DIR}/i18n/messageformat2_function_registry.cpp"
    "${ICU_SOURCE_DIR}/i18n/messageformat2_parser.cpp"
    "${ICU_SOURCE_DIR}/i18n/messageformat2_serializer.cpp"
    "${ICU_SOURCE_DIR}/i18n/msgfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/name2uni.cpp"
    "${ICU_SOURCE_DIR}/i18n/nfrs.cpp"
    "${ICU_SOURCE_DIR}/i18n/nfrule.cpp"
    "${ICU_SOURCE_DIR}/i18n/nfsubs.cpp"
    "${ICU_SOURCE_DIR}/i18n/nortrans.cpp"
    "${ICU_SOURCE_DIR}/i18n/nultrans.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_affixutils.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_asformat.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_capi.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_compact.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_currencysymbols.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_decimalquantity.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_decimfmtprops.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_fluent.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_formatimpl.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_grouping.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_integerwidth.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_longnames.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_mapper.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_modifiers.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_multiplier.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_notation.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_output.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_padding.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_patternmodifier.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_patternstring.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_rounding.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_scientific.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_simple.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_skeletons.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_symbolswrapper.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_usageprefs.cpp"
    "${ICU_SOURCE_DIR}/i18n/number_utils.cpp"
    "${ICU_SOURCE_DIR}/i18n/numfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/numparse_affixes.cpp"
    "${ICU_SOURCE_DIR}/i18n/numparse_compositions.cpp"
    "${ICU_SOURCE_DIR}/i18n/numparse_currency.cpp"
    "${ICU_SOURCE_DIR}/i18n/numparse_decimal.cpp"
    "${ICU_SOURCE_DIR}/i18n/numparse_impl.cpp"
    "${ICU_SOURCE_DIR}/i18n/numparse_parsednumber.cpp"
    "${ICU_SOURCE_DIR}/i18n/numparse_scientific.cpp"
    "${ICU_SOURCE_DIR}/i18n/numparse_symbols.cpp"
    "${ICU_SOURCE_DIR}/i18n/numparse_validators.cpp"
    "${ICU_SOURCE_DIR}/i18n/numrange_capi.cpp"
    "${ICU_SOURCE_DIR}/i18n/numrange_fluent.cpp"
    "${ICU_SOURCE_DIR}/i18n/numrange_impl.cpp"
    "${ICU_SOURCE_DIR}/i18n/numsys.cpp"
    "${ICU_SOURCE_DIR}/i18n/olsontz.cpp"
    "${ICU_SOURCE_DIR}/i18n/persncal.cpp"
    "${ICU_SOURCE_DIR}/i18n/pluralranges.cpp"
    "${ICU_SOURCE_DIR}/i18n/plurfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/plurrule.cpp"
    "${ICU_SOURCE_DIR}/i18n/quant.cpp"
    "${ICU_SOURCE_DIR}/i18n/quantityformatter.cpp"
    "${ICU_SOURCE_DIR}/i18n/rbnf.cpp"
    "${ICU_SOURCE_DIR}/i18n/rbt.cpp"
    "${ICU_SOURCE_DIR}/i18n/rbt_data.cpp"
    "${ICU_SOURCE_DIR}/i18n/rbt_pars.cpp"
    "${ICU_SOURCE_DIR}/i18n/rbt_rule.cpp"
    "${ICU_SOURCE_DIR}/i18n/rbt_set.cpp"
    "${ICU_SOURCE_DIR}/i18n/rbtz.cpp"
    "${ICU_SOURCE_DIR}/i18n/regexcmp.cpp"
    "${ICU_SOURCE_DIR}/i18n/regeximp.cpp"
    "${ICU_SOURCE_DIR}/i18n/regexst.cpp"
    "${ICU_SOURCE_DIR}/i18n/regextxt.cpp"
    "${ICU_SOURCE_DIR}/i18n/region.cpp"
    "${ICU_SOURCE_DIR}/i18n/reldatefmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/reldtfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/rematch.cpp"
    "${ICU_SOURCE_DIR}/i18n/remtrans.cpp"
    "${ICU_SOURCE_DIR}/i18n/repattrn.cpp"
    "${ICU_SOURCE_DIR}/i18n/rulebasedcollator.cpp"
    "${ICU_SOURCE_DIR}/i18n/scientificnumberformatter.cpp"
    "${ICU_SOURCE_DIR}/i18n/scriptset.cpp"
    "${ICU_SOURCE_DIR}/i18n/search.cpp"
    "${ICU_SOURCE_DIR}/i18n/selfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/sharedbreakiterator.cpp"
    "${ICU_SOURCE_DIR}/i18n/simpletz.cpp"
    "${ICU_SOURCE_DIR}/i18n/smpdtfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/smpdtfst.cpp"
    "${ICU_SOURCE_DIR}/i18n/sortkey.cpp"
    "${ICU_SOURCE_DIR}/i18n/standardplural.cpp"
    "${ICU_SOURCE_DIR}/i18n/string_segment.cpp"
    "${ICU_SOURCE_DIR}/i18n/strmatch.cpp"
    "${ICU_SOURCE_DIR}/i18n/strrepl.cpp"
    "${ICU_SOURCE_DIR}/i18n/stsearch.cpp"
    "${ICU_SOURCE_DIR}/i18n/taiwncal.cpp"
    "${ICU_SOURCE_DIR}/i18n/timezone.cpp"
    "${ICU_SOURCE_DIR}/i18n/titletrn.cpp"
    "${ICU_SOURCE_DIR}/i18n/tmunit.cpp"
    "${ICU_SOURCE_DIR}/i18n/tmutamt.cpp"
    "${ICU_SOURCE_DIR}/i18n/tmutfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/tolowtrn.cpp"
    "${ICU_SOURCE_DIR}/i18n/toupptrn.cpp"
    "${ICU_SOURCE_DIR}/i18n/translit.cpp"
    "${ICU_SOURCE_DIR}/i18n/transreg.cpp"
    "${ICU_SOURCE_DIR}/i18n/tridpars.cpp"
    "${ICU_SOURCE_DIR}/i18n/tzfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/tzgnames.cpp"
    "${ICU_SOURCE_DIR}/i18n/tznames.cpp"
    "${ICU_SOURCE_DIR}/i18n/tznames_impl.cpp"
    "${ICU_SOURCE_DIR}/i18n/tzrule.cpp"
    "${ICU_SOURCE_DIR}/i18n/tztrans.cpp"
    "${ICU_SOURCE_DIR}/i18n/ucal.cpp"
    "${ICU_SOURCE_DIR}/i18n/ucln_in.cpp"
    "${ICU_SOURCE_DIR}/i18n/ucol.cpp"
    "${ICU_SOURCE_DIR}/i18n/ucol_res.cpp"
    "${ICU_SOURCE_DIR}/i18n/ucol_sit.cpp"
    "${ICU_SOURCE_DIR}/i18n/ucoleitr.cpp"
    "${ICU_SOURCE_DIR}/i18n/ucsdet.cpp"
    "${ICU_SOURCE_DIR}/i18n/udat.cpp"
    "${ICU_SOURCE_DIR}/i18n/udateintervalformat.cpp"
    "${ICU_SOURCE_DIR}/i18n/udatpg.cpp"
    "${ICU_SOURCE_DIR}/i18n/ufieldpositer.cpp"
    "${ICU_SOURCE_DIR}/i18n/uitercollationiterator.cpp"
    "${ICU_SOURCE_DIR}/i18n/ulistformatter.cpp"
    "${ICU_SOURCE_DIR}/i18n/ulocdata.cpp"
    "${ICU_SOURCE_DIR}/i18n/umsg.cpp"
    "${ICU_SOURCE_DIR}/i18n/unesctrn.cpp"
    "${ICU_SOURCE_DIR}/i18n/uni2name.cpp"
    "${ICU_SOURCE_DIR}/i18n/units_complexconverter.cpp"
    "${ICU_SOURCE_DIR}/i18n/units_converter.cpp"
    "${ICU_SOURCE_DIR}/i18n/units_data.cpp"
    "${ICU_SOURCE_DIR}/i18n/units_router.cpp"
    "${ICU_SOURCE_DIR}/i18n/unum.cpp"
    "${ICU_SOURCE_DIR}/i18n/unumsys.cpp"
    "${ICU_SOURCE_DIR}/i18n/upluralrules.cpp"
    "${ICU_SOURCE_DIR}/i18n/uregex.cpp"
    "${ICU_SOURCE_DIR}/i18n/uregexc.cpp"
    "${ICU_SOURCE_DIR}/i18n/uregion.cpp"
    "${ICU_SOURCE_DIR}/i18n/usearch.cpp"
    "${ICU_SOURCE_DIR}/i18n/uspoof.cpp"
    "${ICU_SOURCE_DIR}/i18n/uspoof_build.cpp"
    "${ICU_SOURCE_DIR}/i18n/uspoof_conf.cpp"
    "${ICU_SOURCE_DIR}/i18n/uspoof_impl.cpp"
    "${ICU_SOURCE_DIR}/i18n/utf16collationiterator.cpp"
    "${ICU_SOURCE_DIR}/i18n/utf8collationiterator.cpp"
    "${ICU_SOURCE_DIR}/i18n/utmscale.cpp"
    "${ICU_SOURCE_DIR}/i18n/utrans.cpp"
    "${ICU_SOURCE_DIR}/i18n/vtzone.cpp"
    "${ICU_SOURCE_DIR}/i18n/vzone.cpp"
    "${ICU_SOURCE_DIR}/i18n/windtfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/winnmfmt.cpp"
    "${ICU_SOURCE_DIR}/i18n/wintzimpl.cpp"
    "${ICU_SOURCE_DIR}/i18n/zonemeta.cpp"
    "${ICU_SOURCE_DIR}/i18n/zrule.cpp"
    "${ICU_SOURCE_DIR}/i18n/ztrans.cpp")

    file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/empty.cpp" CONTENT " ")
    enable_language(ASM)

    if (CMAKE_SYSTEM_NAME MATCHES "Darwin|Linux")
        set(ICUDATA_SOURCE_FILE "${ICUDATA_SOURCE_DIR}/icudt75l_dat.S")
    else ()
        message(FATAL_ERROR "Unsupported operating system. This system is not supported by this project. Please check your environment settings.")
    endif ()

    set(ICUDATA_SOURCES
        "${ICUDATA_SOURCE_FILE}"
        "${CMAKE_CURRENT_BINARY_DIR}/empty.cpp"
    )

    add_definitions(-D_REENTRANT -DU_HAVE_ELF_H=1 -DU_HAVE_STRTOD_L=1 -DU_HAVE_XLOCALE_H=0 -DDEFAULT_ICU_PLUGINS="/dev/null")

    add_library(_icuuc ${ICUUC_SOURCES})
    add_library(_icui18n ${ICUI18N_SOURCES})
    add_library(_icudata ${ICUDATA_SOURCES})

    target_link_libraries(_icuuc PRIVATE _icudata)
    target_link_libraries(_icui18n PRIVATE _icuuc)

    target_include_directories(_icuuc SYSTEM PUBLIC "${ICU_SOURCE_DIR}/common/")
    target_include_directories(_icui18n SYSTEM PUBLIC "${ICU_SOURCE_DIR}/i18n/")

    target_compile_definitions(_icuuc PRIVATE -DU_COMMON_IMPLEMENTATION)
    target_compile_definitions(_icui18n PRIVATE -DU_I18N_IMPLEMENTATION)

    add_library(icu INTERFACE)
    target_link_libraries(icu INTERFACE _icui18n _icuuc _icudata)
else()
  message(STATUS "ICU target already exists")
endif()