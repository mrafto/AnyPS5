#include "prx/libc/include/CpuTopology.hpp"
#include <cstdio>
#include <cstdlib>
#include <vector>

static void Require(bool value, const char* what) {
    if (value) return;
    std::fprintf(stderr, "cpu topology layout test failed: %s\n", what);
    std::exit(1);
}

static CpuTopology::Layout Classify(const std::vector<CpuTopology::Core>& cores, std::uint64_t process = 0xffff) {
    return CpuTopology::Classify(cores.data(), cores.size(), process);
}

int main() {
    const auto single = Classify({{0, 0x0f, 1}, {0, 0xf0, 0}});
    Require(single.hybrid && single.performant == 0x0f && single.efficient == 0xf0 && single.group == 0, "one group with two classes");

    const auto sameClass = Classify({{0, 0x0f, 1}, {0, 0xf0, 1}});
    Require(!sameClass.hybrid && sameClass.efficient == 0 && sameClass.performant == 0, "one class is not hybrid");

    const auto three = Classify({{0, 0x03, 0}, {0, 0x3c, 1}, {0, 0xc0, 2}});
    Require(three.hybrid && three.efficient == 0x03 && three.performant == 0xc0, "middle class is left out");

    const auto secondGroup = Classify({{1, 0x0f, 1}, {1, 0xf0, 0}});
    Require(secondGroup.hybrid && secondGroup.group == 1 && secondGroup.performant == 0x0f && secondGroup.efficient == 0xf0, "both classes in group 1");

    const auto split = Classify({{0, 0x0f, 1}, {1, 0xf0, 0}});
    Require(!split.hybrid && split.efficient == 0 && split.performant == 0 && split.process == 0xffff, "classes in different groups cannot be represented");

    const auto interleaved = Classify({{0, 0x01, 0}, {0, 0x02, 1}, {1, 0x01, 0}});
    Require(!interleaved.hybrid, "a class that spans groups cannot be represented");

    const auto performantSpans = Classify({{0, 0x01, 0}, {0, 0x02, 1}, {1, 0x04, 1}});
    Require(!performantSpans.hybrid, "a performant class that spans groups cannot be represented");

    const auto ignoredMiddleGroup = Classify({{0, 0x01, 0}, {0, 0x02, 2}, {1, 0x04, 1}});
    Require(ignoredMiddleGroup.hybrid && ignoredMiddleGroup.group == 0 && ignoredMiddleGroup.efficient == 0x01 && ignoredMiddleGroup.performant == 0x02, "a middle class in another group does not matter");

    Require(!Classify({}).hybrid, "no cores");
    Require(!Classify({{0, 0, 0}, {0, 0, 1}}).hybrid, "empty masks");
    Require(!Classify({{0, 0xff, 300}}).hybrid, "class out of range");

    CpuTopology::Layout grouped;
    grouped.process = 0x0f;
    grouped.group = 0;
    Require(CpuTopology::ClipToProcess(grouped, 0xff) == 0x0f, "group 0 is clipped to the process");
    grouped.group = 1;
    Require(CpuTopology::ClipToProcess(grouped, 0xff) == 0xff, "another group is not clipped to the group 0 process mask");
    grouped.group = 0;
    grouped.process = 0;
    Require(CpuTopology::ClipToProcess(grouped, 0xff) == 0xff, "unknown process affinity does not clip");

    Require(CpuTopology::ValidRole(0) && CpuTopology::ValidRole(1) && !CpuTopology::ValidRole(2), "known roles");
    return 0;
}
