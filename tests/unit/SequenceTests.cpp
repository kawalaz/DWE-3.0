#include "dwe/core/Sequence.h"

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <set>

namespace {
int failures = 0;
void check(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "FAIL: " << message << '\n';
        ++failures;
    }
}
}

int main() {
    using namespace dwe::core;
    using R = SequenceWindow::Result;

    // Serial arithmetic, including across the 32-bit wrap.
    check(seq_before(1, 2) && seq_after(2, 1) && !seq_before(2, 1), "simple ordering");
    check(!seq_before(5, 5) && !seq_after(5, 5), "equal numbers are neither before nor after");
    check(seq_before(0xFFFFFFFFu, 0), "0xFFFFFFFF is before 0 after wrap");
    check(seq_after(1, 0xFFFFFFF0u), "1 is after 0xFFFFFFF0 after wrap");
    check(seq_before(0xFFFFFFF0u, 5) && !seq_before(5, 0xFFFFFFF0u), "ordering is symmetric across wrap");

    // SequenceWindow: duplicates, reordering inside the window, too old.
    SequenceWindow w;
    check(w.check_and_mark(100) == R::Accepted, "first sequence accepted");
    check(w.check_and_mark(100) == R::Duplicate, "immediate duplicate rejected");
    check(w.check_and_mark(101) == R::Accepted, "next sequence accepted");
    check(w.check_and_mark(99) == R::Accepted, "reordered older sequence inside window accepted once");
    check(w.check_and_mark(99) == R::Duplicate, "reordered sequence is a duplicate the second time");
    check(w.check_and_mark(101) == R::Duplicate, "highest sequence is a duplicate on replay");
    check(w.check_and_mark(150) == R::Accepted, "jump forward inside window width accepted");
    check(w.check_and_mark(100) == R::Duplicate, "old accepted sequence still remembered after small jump");
    check(w.check_and_mark(120) == R::Accepted, "never-seen sequence inside window accepted");
    check(w.check_and_mark(86) == R::TooOld, "sequence exactly one width behind the highest is too old");
    check(w.check_and_mark(87) == R::Accepted, "oldest sequence still inside the window accepted");
    check(w.check_and_mark(10) == R::TooOld, "far older sequence rejected");
    check(w.check_and_mark(150 + 1000) == R::Accepted, "large jump forward resets the window");
    check(w.check_and_mark(150) == R::TooOld, "pre-jump sequence is now too old");

    // Wraparound: a window straddling 0xFFFFFFFF -> 0 must behave like any other.
    SequenceWindow wrap;
    check(wrap.check_and_mark(0xFFFFFFFEu) == R::Accepted, "wrap: first");
    check(wrap.check_and_mark(0xFFFFFFFFu) == R::Accepted, "wrap: next");
    check(wrap.check_and_mark(0) == R::Accepted, "wrap: crossing zero is newer");
    check(wrap.check_and_mark(1) == R::Accepted, "wrap: after zero");
    check(wrap.check_and_mark(0xFFFFFFFEu) == R::Duplicate, "wrap: pre-wrap sequence is a duplicate");
    check(wrap.check_and_mark(0xFFFFFFFFu) == R::Duplicate, "wrap: pre-wrap sequence 2 is a duplicate");
    check(wrap.check_and_mark(0) == R::Duplicate, "wrap: zero is a duplicate");
    check(wrap.highest() == 1, "wrap: highest advanced across wrap");

    // Exhaustive property: every sequence in a long run is accepted exactly once even if replayed.
    SequenceWindow run;
    std::set<std::uint32_t> accepted;
    const std::uint32_t start = 0xFFFFFF00u;  // crosses the wrap
    for (std::uint32_t i = 0; i < 1000; ++i) {
        const std::uint32_t s = start + i;
        for (int replay = 0; replay < 3; ++replay) {
            if (run.check_and_mark(s) == R::Accepted) accepted.insert(s);
        }
    }
    check(accepted.size() == 1000, "each of 1000 consecutive sequences accepted exactly once across wrap");

    // NewestSequence.
    NewestSequence n;
    check(n.accept(5), "newest: first accepted");
    check(!n.accept(5), "newest: duplicate dropped");
    check(!n.accept(4), "newest: older dropped");
    check(n.accept(7), "newest: jump forward accepted (6 is skipped, not retransmitted)");
    check(!n.accept(6), "newest: late 6 is stale and dropped");
    NewestSequence nw;
    check(nw.accept(0xFFFFFFFFu) && nw.accept(0) && !nw.accept(0xFFFFFFFFu), "newest: wraps correctly");

    if (failures == 0) std::cout << "All DWE sequence tests passed.\n";
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
