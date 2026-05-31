#ifndef FMI_FT_OPERATIONRUNTIME_H
#define FMI_FT_OPERATIONRUNTIME_H

namespace FMI::FT {
    class OperationRuntime {
    public:
        virtual ~OperationRuntime() = default;
        virtual void enter_operation() = 0;
        virtual void exit_operation() = 0;
        virtual void shutdown() {}
    };
}

#endif //FMI_FT_OPERATIONRUNTIME_H
