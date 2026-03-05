#ifndef KORO_InplaceFunction_HPP
#define KORO_InplaceFunction_HPP

#include "koro/noncopyable.h"
#include <cstddef>
#include <utility>

namespace koro
{
template <size_t BufferSize = 64> class InplaceFunction : public noncopyable
{
  private:
	alignas(std::max_align_t) char buffer_[BufferSize]{0};

	const struct VTable
	{
		void (*invoke)(void* ptr);
		void (*move)(void* dest, void* src);
		void (*destroy)(void* ptr);
	}* vtable_{nullptr};

	template <typename F> static const VTable* vtable()
	{
		static constexpr VTable vtable{
			// Invoke
			[](void* ptr) { (*reinterpret_cast<F*>(ptr))(); },
			// Move construct
			[](void* dest, void* src)
			{ new (dest) F(std::move(*reinterpret_cast<F*>(src))); },
			// Destroy
			[](void* ptr) { reinterpret_cast<F*>(ptr)->~F(); }};
		return &vtable;
	}

  public:
	InplaceFunction() : vtable_(nullptr)
	{
	}

	~InplaceFunction()
	{
		if (vtable_)
		{
			vtable_->destroy(buffer_);
		}
	}

	template <typename F>
		requires(!std::is_same_v<std::decay_t<F>, InplaceFunction>)
	InplaceFunction(F&& f)
	{
		using DecayedF = std::decay_t<F>;
		static_assert(sizeof(DecayedF) <= BufferSize, "callable too large");
		new (buffer_) DecayedF(std::forward<F>(f));
		vtable_ = vtable<DecayedF>();
	}

	InplaceFunction(const InplaceFunction&) = delete;
	InplaceFunction& operator=(const InplaceFunction&) = delete;

	InplaceFunction(InplaceFunction&& other) noexcept
	{
		if (other.vtable_)
		{
			other.vtable_->move(buffer_, other.buffer_);
			vtable_ = other.vtable_;
			other.vtable_ = nullptr;
		}
		else
		{
			vtable_ = nullptr;
		}
	}

	InplaceFunction& operator=(InplaceFunction&& other) noexcept
	{
		if (&other == this)
		{
			return *this;
		}

		if (vtable_)
		{
			vtable_->destroy(buffer_);
		}

		if (other.vtable_)
		{
			other.vtable_->move(buffer_, other.buffer_);
			vtable_ = other.vtable_;
			other.vtable_ = nullptr;
		}
		else
		{
			vtable_ = nullptr;
		}
		return *this;
	}

	void operator()()
	{
		if (vtable_)
		{
			vtable_->invoke(buffer_);
		}
	}

	explicit operator bool() const noexcept
	{
		return vtable_ != nullptr;
	}
};
} // namespace koro

#endif // !InplaceFunction
