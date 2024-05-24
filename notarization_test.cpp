#include "streamlet.grpc.pb.h"

#include <iostream>
#include <mutex>
#include <algorithm>
#include <stdexcept>
#include <sstream>
#include <list>
#include <vector>

#include "structs.h"

// Add underscore to differentiate from standard library assert
#define _assert(cond, errmsg) if (!(cond)) throw std::logic_error{(errmsg)}

/*
 * Class that tests the notarization procedure in StreamletNodeV1 by allowing the
 * notarization of blocks to be mocked up. Contains a copy of the private members
 * in StreamletNodeV1 to 
 */
class NotarizationTest {
public:
    NotarizationTest();

    std::list<const ChainElement*> notarize(uint64_t note_epoch, uint64_t par_epoch);

    const ChainElement *get_element(uint64_t epoch);

    uint64_t get_max_chainlen();

    const ChainElement *get_last_finalized();

    const ChainElement *get_genesis_block();

private:
    std::list<const ChainElement*> notarize_block(
        const Block &note_block,
        const std::string &note_hash,
        uint64_t note_epoch,
        uint64_t par_epoch
    );

    // Mapping from a block to its dependents so that the chain can
    // be constructed with later blocks that were notarized first and
    // were waiting on a given earlier block
    std::unordered_map<uint64_t, ChainElement*> successors;

    // Length of longest notarized chain, also guarded by successors_m
    uint64_t max_chainlen;

    // Pointer to one past last finalized ChainElement in successors,
    // which is the block at which to start notifying the client app
    // of new finalizations, also guarded by successors_m
    const ChainElement *last_finalized;

    // Mutex for successors
    std::mutex successors_m;

    // Genesis block
    ChainElement genesis_block;
};

NotarizationTest::NotarizationTest() : max_chainlen{0}, genesis_block{0} {
    // Create entry for dependents of genesis block
    successors.emplace(0, &genesis_block);

    // Set pointer once genesis block is constructed
    last_finalized = &genesis_block;
}

std::list<const ChainElement*> NotarizationTest::notarize(uint64_t note_epoch, uint64_t par_epoch) {
    Block b;
    b.set_parent(par_epoch);
    b.mutable_phash()->resize(32);
    fill(b.mutable_phash()->begin(), b.mutable_phash()->end(), 0);
    b.set_epoch(note_epoch);
    b.mutable_txns()->resize(1024);
    fill(b.mutable_txns()->begin(), b.mutable_txns()->end(), 1);

    // hash is all zeros anyway so reuse the string stored in b
    return notarize_block(b, b.phash(), note_epoch, par_epoch);
}

const ChainElement *NotarizationTest::get_element(uint64_t epoch) {
    std::unordered_map<uint64_t, ChainElement*>::iterator search
        = successors.find(epoch);

    if (search == successors.end()) {
        std::ostringstream s;
        s << "No element in successors for epoch " << epoch;
        throw std::out_of_range{s.str()};
    }

    return search->second;
}

uint64_t NotarizationTest::get_max_chainlen() {
    return max_chainlen;
}

const ChainElement *NotarizationTest::get_last_finalized() {
    return last_finalized;
}

const ChainElement *NotarizationTest::get_genesis_block() {
    return &genesis_block;
}

std::list<const ChainElement*> NotarizationTest::notarize_block(
    const Block &note_block,
    const std::string &note_hash,
    uint64_t note_epoch,
    uint64_t par_epoch
) {
    std::list<ChainElement*> queue;
    const ChainElement *prev_finalized = nullptr;
    ChainElement *new_finalized = nullptr;

    std::cout << "Notarizing block " << note_epoch << std::endl;

    if (note_epoch == 0) {
        std::cerr << "Genesis block should never be passed to notarize_block" << std::endl;
        return {};
    }

    successors_m.lock();

    // In the time that ProposeBlock was running, it is possible that a block on a different branch
    // has extended the finalized chain, in which case a ChainElement for note_epoch must not be
    // constructed because the ChainElement at par_epoch has been deleted and the new ChainElement
    // would not later be garbage collected.
    if (par_epoch < last_finalized->block.epoch()) {
        successors_m.unlock();
        return {};
    }

    ChainElement *&p_elem = successors[par_epoch];
    if (p_elem == nullptr) {
        p_elem = new ChainElement{par_epoch};
        std::cout << "Created entry for parent (block " << par_epoch << ')' << std::endl;
    }

    ChainElement *&new_elem = successors[note_epoch];
    if (new_elem == nullptr) {
        new_elem = new ChainElement{note_epoch};
        new_elem->block.CopyFrom(note_block);
        new_elem->hash = note_hash;
        std::cout << "Created entry"  << std::endl;
    } else if (new_elem->index == 0) {
        // ChainElement already existed but block was previously not notarized
        new_elem->block.CopyFrom(note_block);
        new_elem->hash = note_hash;
        std::cout << "Using existing entry" << std::endl;
    } else {
        successors_m.unlock();
        std::cerr << "Duplicate attempt to notarize block " << note_epoch << std::endl;
        return {};
    }

    new_elem->plink = p_elem;

    p_elem->links.push_back(new_elem);

    // If a finalization is detected, this is set to reference the last element
    // of a sequence of three or more contiguous epochs
    std::list<ChainElement*>::iterator finalize_end = queue.end();

    // If the parent is already linked into the chain, or the parent is the genesis
    // block, then set the chain index of new_elem and look for any children
    // that can be linked and check if they extend the longest chain at their position.
    if (p_elem->index != 0 || par_epoch == 0) {
        new_elem->index = p_elem->index + 1;

        queue.push_back(new_elem);

        // Check whether new_elem itself ends a finalized chain
        if (new_elem->epoch == p_elem->epoch + 1
            && (p_elem->plink != nullptr && p_elem->epoch == p_elem->plink->epoch + 1)) {
            finalize_end = queue.begin();
        }

        // Run a BFS to relink previously notarized blocks and detect finalization
        std::list<ChainElement*>::iterator queue_iter = queue.begin();
        while (queue_iter != queue.end()) {
            const ChainElement *elem = *queue_iter;
            bool parent_contiguous = elem->epoch == elem->plink->epoch + 1;
            std::cout << "Block " << elem->epoch << " contiguous with " << elem->plink->epoch
                << " when running bfs" << std::endl;

            for (ChainElement *child : elem->links) {
                child->index = elem->index + 1;

                queue.push_back(child);

                // If the child's epoch is contiguous with its parent's, and the parent
                // was contiguous with the grandparent, then the child is now the first
                // non-finalized element.
                if (parent_contiguous && child->epoch == elem->epoch + 1) {
                    finalize_end = --queue.end();
                }
            }

            ++queue_iter;
        }

        // Because queue is expanded in order of increasing BFS depth, the
        // last element must contain the length of the longest notarized
        // chain, although this chain is not necessarily unique
        max_chainlen = queue.back()->index;
    }

    if (finalize_end != queue.end()) {
        // The last finalized element is second to last element in the
        // contiguous sequence of epochs. This will be used to update
        // last_finalized below if the tail of the finalized chain can
        // be walked all the way back to last_finalized.
        new_finalized = (*finalize_end)->plink;

        queue.clear();

        std::list<ChainElement*>::iterator queue_iter = queue.end();

        // In this branch, since finalize_end was set, its plink is known
        // not to be null as is its grandparent (*finalize_end)->plink->plink,
        // so that the first iteration of the loop below is safe
        ChainElement *elem = new_finalized;

        while (elem != last_finalized && elem->index != 0) {
            std::cout << "Traversing finalized chain back from block " << elem->epoch << std::endl;

            ChainElement *p = elem->plink;
            std::list<ChainElement*>::iterator pchild = p->links.begin();
            std::list<ChainElement*>::iterator rm;

            // This loop pushes siblings of elem, which are now known not to be
            // in the finalized chain, into queue to be deleted below
            while (pchild != p->links.end()) {
                if (*pchild != elem) {
                    queue.push_back(*pchild);
                    rm = pchild;
                    ++pchild;
                    p->links.erase(rm);
                } else {
                    ++pchild;
                }
            }

            std::cout << "  parent (block " << p->epoch << "'s) successors:" ;
            for (ChainElement *child : p->links) {
                std::cout << ' ' << child->epoch;
            }
            std::cout << std::endl;

            // queue_iter will still be queue.end() on next iteration
            // if nothing was added to queue
            if (queue_iter == queue.end()) {
                queue_iter = queue.begin();
            } else {
                ++queue_iter;
            }

            // Run a BFS over the added elements to collect all elements
            // that are known not to be in the finalized chain
            while (queue_iter != queue.end()) {
                for (ChainElement *child : (*queue_iter)->links) {
                    queue.push_back(child);
                }
                ++queue_iter;
            }

            // Move iterator back to last element in queue so that it can
            // continue from the given position to newly added elements
            // in the next iteration of the outer while loop
            --queue_iter;

            elem = p;
        }

        if (elem == last_finalized) {
            prev_finalized = last_finalized;

            last_finalized = new_finalized;

            // Remove outdated chains from the successors map
            for (ChainElement *rm : queue) {
                successors.erase(rm->epoch);
            }
        }
    }

    successors_m.unlock();

    std::list<const ChainElement*> finalized_elems;

    if (prev_finalized != nullptr) {
        // Deletion can be done outside the lock since elements are deleted
        // only when last_finalized has been updated, so no later blocks
        // can be notarized on any chain not extending from the new last
        // finalized block.
        for (ChainElement *elem : queue) {
            delete elem;
        }

        // Notify state machine client of finalized transactions. Similar to
        // the above, no new messages will change the state of these elements.
        // Moreover, notification is read-only, so notification does not require
        // taking hold of the lock. There also should be only one child per
        // element in the finalized chain, and the notifications begin from the
        // child of the previous last finalized element.
        if (prev_finalized->links.size() != 1) {
            std::cerr << "Block " <<  prev_finalized->epoch
                << " on finalized chain does not have exactly one successor" << std::endl;
        }

        do {
            prev_finalized = prev_finalized->links.front();

            // Record the finalized elements that a client would have been
            // notified about in order so that this can be verified
            finalized_elems.push_back(prev_finalized);

        } while (prev_finalized != new_finalized);
    }

    return finalized_elems;
}

/*
 * Simulates the following chain
 *
 *   1 - 3 - 5 - 8 - 9 - 10 - 11 - 13
 *  /     \               \
 * 0       6 - 7           12
 *  \
 *   2 - 4
 */
void test1 () {
    NotarizationTest test;

    _assert(test.get_element(0) == test.get_genesis_block(),
        "test1: block 0 should be in successors and refer to genesis block");

    test.notarize(1, 0);
    test.notarize(2, 0);
    test.notarize(3, 1);
    test.notarize(4, 2);

    test.notarize(5, 3);
    test.notarize(6, 3);
    test.notarize(7, 6);

    _assert(test.get_max_chainlen() == 4,
            "test1: max_chainlen should be 4");

    const ChainElement *block3 = test.get_element(3);
    const ChainElement *block5 = test.get_element(5);
    const ChainElement *block6 = test.get_element(6);
    const ChainElement *block7 = test.get_element(7);
    _assert(std::find(block3->links.begin(), block3->links.end(), block5) != block3->links.end(),
            "test1: block 3 should have block 5 in links");
    _assert(std::find(block3->links.begin(), block3->links.end(), block6) != block3->links.end(),
            "test1: block 3 should have block 6 in links");
    _assert(std::find(block6->links.begin(), block6->links.end(), block7) != block6->links.end(),
            "test1: block 6 should have block 7 in links");

    test.notarize(8, 5);
    test.notarize(10, 9); // 10 arrives before 9

    // 8's index should be known
    _assert(test.get_element(8)->index == 4,
            "test1: block 8 should be at index 4");

    // There is now a gap between 8 and 10. Check that 9
    // is in successors has been initialized to an empty
    // parent (i.e. contains no block). Also check that
    // 10's chain index is 0 since 9 has not yet been seen.
    _assert(test.get_element(9)->index == 0,
            "test1: block 9 index should be 0");
    _assert(test.get_element(9)->plink == nullptr,
            "test1: block 9 block should be uninitialized");
    _assert(test.get_element(10)->index == 0,
            "test1: block 10 index should be 0");
    _assert(test.get_element(10)->plink != nullptr,
            "test1: block 10 block should be initialized");

    test.notarize(13, 11); // 13 arrives before 11

    // There is now a gap between 13 and 10. Perform a
    // similar check as above. max_chainlen should
    // still be 4 since 9 is missing.
    _assert(test.get_element(11)->index == 0,
            "test1: block 9 index should be 0");
    _assert(test.get_element(11)->plink == nullptr,
            "test1: block 9 block should be uninitialized");
    _assert(test.get_element(13)->index == 0,
            "test1: block 10 index should be 0");
    _assert(test.get_element(13)->plink != nullptr,
            "test1: block 10 block should be initialized");

    test.notarize(12, 10);
    test.notarize(11, 10);

    // 9 should link subtree at 10 to genesis
    std::list<const ChainElement*> finalized
        = test.notarize(9, 8);

    // 8's index was checked above so 9's index should also be known
    _assert(test.get_element(9)->index == 5,
            "test1: block 9 index should be at index 5");
    _assert(test.get_max_chainlen() == 8,
            "test1: max_chainlen should be 8");

    // last_finalized should now be 10
    _assert(test.get_last_finalized()->epoch == 10,
            "test1: last_finalized should be block 10");
    _assert(test.get_last_finalized()->index == 6,
            "test1: last_finalized should be at index 6");

    // Check values were deleted
    bool not_found = false;
    try {
        const ChainElement *block2 = test.get_element(2);
        std::cout << block2->epoch << std::endl;
    } catch (const std::out_of_range &e) {
        not_found = true;
    }
    _assert(not_found, "test1: block 2 should have been removed from successors");

    not_found = false;
    try {
        const ChainElement *block4 = test.get_element(4);
        std::cout << block4->epoch << std::endl;
    } catch (const std::out_of_range &e) {
        not_found = true;
    }
    _assert(not_found, "test1: block 4 should have been removed from successors");

    const ChainElement *block1 = test.get_element(1);
    _assert(test.get_genesis_block()->links.size() == 1,
            "test1: genesis block should now have one successor");
    _assert(test.get_genesis_block()->links.front() == block1,
            "test1: genesis block should now be succeeded by block 1 only");

    // Check finalized chain
    std::ostringstream s;
    std::vector<uint64_t> chain{1, 3, 5, 8, 9, 10};
    std::list<const ChainElement*>::iterator final_iter = finalized.begin();
    for (uint64_t i = 0; i < chain.size() - 1; i++) {
        const ChainElement *b = test.get_element(chain[i]);
        const ChainElement *c = test.get_element(chain[i + 1]);

        s << "test1: block " << b->epoch << " not found at expected position in new "
            << "segment of finalized chain" << std::endl;
        _assert(b == *final_iter, s.str());
        s.str("");

        s << "test1: block " << b->epoch << " should now have one successor";
        _assert(b->links.size() == 1, s.str());
        s.str("");

        s << "test1: block " << b->epoch << " should now be succeeded by block "
            << c->epoch << " only";
        _assert(b->links.front() == c, s.str());
        s.str("");

        ++final_iter;
    }

    _assert(*final_iter == test.get_element(10),
        "test1: finalized chain should contain block 10");
    _assert(++final_iter == finalized.end(),
        "test1: finalized chain should end with block 10");
}

/*
 * Simulates the following chain where 10 arrives late
 * then 4 arrives even later to check the finalization that
 * occurs at 10 can be linked later to the full chain when
 * the finalization at 4 arrives.
 *
 * 0 - 1 - 4 - 5 - 6 - 8 - 10 - 11 - 12 - 13
 *      \                              \
 *       3                              14
 *
 */
void test2() {
    NotarizationTest test;

    _assert(test.get_element(0) == test.get_genesis_block(),
        "test2: block 0 should be in successors and refer to genesis block");

    test.notarize(1, 0);
    test.notarize(3, 1);

    _assert(test.get_max_chainlen() == 2,
            "test2: max_chainlen should be 2");

    // 4 is delayed
    test.notarize(5, 4);
    test.notarize(6, 5);
    test.notarize(8, 6);

    // 10 is delayed
    test.notarize(11, 10);
    test.notarize(12, 11);
    test.notarize(13, 12);
    test.notarize(14, 12);

    test.notarize(10, 8);

    // The chain from 5 through 12 is now final. However,
    // it cannot be linked back to the genesis block, so
    // last_finalized should still be at 0, as is max_chainlen.
    _assert(test.get_last_finalized() == test.get_genesis_block(),
            "test2: last_finalized should be genesis block");
    _assert(test.get_max_chainlen() == 2,
            "test2: max_chainlen should still be 2");

    _assert(test.get_element(13)->index == 0,
            "test2: block 13 should have index set to 0");
    _assert(test.get_element(14)->index == 0,
            "test2: block 14 should have index set to 0");

    // 13 and 14 are not final so they should still be
    // successors of 12

    std::list<const ChainElement*> finalized
        = test.notarize(4, 1);
    
    // The chain from 0 to 12 is now final. last_finalized
    // and max_chainlen should now be updated. 1 should no
    // longer be linked to 3 as a successor.
    _assert(test.get_last_finalized()->epoch == 12,
            "test2: last_finalized should be block 12");
    _assert(test.get_last_finalized()->index == 8,
            "test2: last_finalized should be at index 8");
    _assert(test.get_element(13)->index == 9,
            "test2: block 13 should now be at index 9");
    _assert(test.get_element(14)->index == 9,
            "test2: block 14 should now be at index 9");

    // Check finalized chain
    std::ostringstream s;
    std::vector<uint64_t> chain{1, 4, 5, 6, 8, 10, 11, 12};
    std::list<const ChainElement*>::iterator final_iter = finalized.begin();
    for (uint64_t i = 0; i < chain.size() - 1; i++) {
        const ChainElement *b = test.get_element(chain[i]);
        const ChainElement *c = test.get_element(chain[i + 1]);

        s << "test2: block " << b->epoch << " not found at expected position in new "
            << "segment of finalized chain" << std::endl;
        _assert(b == *final_iter, s.str());
        s.str("");

        s << "test2: block " << b->epoch << " should now have one successor";
        _assert(b->links.size() == 1, s.str());
        s.str("");

        s << "test2: block " << b->epoch << " should now be succeeded by block "
            << c->epoch << " only";
        _assert(b->links.front() == c, s.str());
        s.str("");

        ++final_iter;
    }

    _assert(*final_iter == test.get_element(12),
        "test2: finalized chain should contain block 12");
    _assert(++final_iter == finalized.end(),
        "test2: finalized chain should end with block 12");
}

/*
 * Simulates the following basic chain to check edge cases on traversing
 * the genesis block when epoch 2 finalizes block 1.
 *
 * 0 - 1 - 2
 */
void test3() {
     NotarizationTest test;

    _assert(test.get_element(0) == test.get_genesis_block(),
        "test3: block 0 should be in successors and refer to genesis block");

    test.notarize(1, 0);

    _assert(test.get_last_finalized() == test.get_genesis_block(),
            "test3: last_finalized should be genesis block");
    _assert(test.get_max_chainlen() == 1,
            "test3: max_chainlen should be 1");

    std::list<const ChainElement*> finalized
        = test.notarize(2, 1);

    _assert(test.get_last_finalized()->epoch,
            "test3: last_finalized should be block 2");
    _assert(test.get_max_chainlen() == 2,
            "test3: max_chainlen should be 2");
}

int main() {
    std::cout << "======== Test 1 ========" << std::endl;
    test1();
    std::cout << "======== Test 2 ========" << std::endl;
    test2();
    std::cout << "======== Test 3 ========" << std::endl;
    test3();
    return 0;
}