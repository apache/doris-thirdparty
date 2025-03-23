#pragma once

CL_NS_DEF(index)

enum class CodeMode { 
  kDefault = 0,
  kPfor = 1,
  kRange = 2,
  kPfor256 = 3,
  kPfor128 = 4
};

CL_NS_END