add_executable(ceph_test_cls_datasyncmap
  test_cls_datasyncmap.cc
  )
target_link_libraries(ceph_test_cls_datasyncmap
  libneorados
  ${BLKID_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${CRYPTO_LIBS}
  ${EXTRALIBS}
  neoradostest-support
  ${UNITTEST_LIBS}
  )
install(TARGETS
  ceph_test_cls_datasyncmap
  DESTINATION ${CMAKE_INSTALL_BINDIR})
